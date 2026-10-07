//! The chat example Worker: sign-in, short-lived access tokens, and a chat room that only
//! signed-in users can open.
//!
//! | Route | Action |
//! | --- | --- |
//! | `GET /partyline/chat/{room}` | WebSocket upgrade. Needs an access token in `token` |
//! | `POST /api/login` | Sign in with a name: set the session cookie, return an access token |
//! | `GET /api/token` | A fresh access token for the session cookie's user |
//! | `POST /api/logout` | Clear the session cookie and close the user's sockets with 4403 |
//! | `GET /api/head` | The room's channel head |
//! | `POST /api/messages` | Post a message as the session cookie's user |
//!
//! The session cookie stands in for an identity provider's session, and `/api/token` for
//! its token call, such as Clerk's `getToken()`. Anyone can sign in as any name: replace
//! `/api/login` with a real identity provider. See docs/how-to/authenticate-with-clerk.md.

mod token;

use chat_shared::{
    AccessToken, BINDING, Chat, ChatEvent, HISTORY, Login, MAX_MESSAGE_LEN, MAX_NAME_LEN,
    PostMessage, ROOM,
};
use partyline_worker::{Connect, HubConfig, Publisher, close, decode_segment, reject};
use token::Kind;
use worker::*;

/// How long an access token is valid. Short, so the 4401 path is easy to see.
const ACCESS_TTL_SECS: u64 = 60;

/// How long a session lasts.
const SESSION_TTL_SECS: u64 = 8 * 60 * 60;

const SESSION_COOKIE: &str = "session";

partyline_worker::channel_object! {
    /// The room's Durable Object. The channel log is the message history.
    pub struct ChatChannel: Chat {
        config = HubConfig::default().retain_events(HISTORY);
    }
}

fn now() -> u64 {
    Date::now().as_millis() / 1000
}

/// The signing key, from the `SESSION_SECRET` secret.
fn secret(env: &Env) -> Result<Vec<u8>> {
    let secret = env.secret("SESSION_SECRET")?.to_string();
    if secret.len() < 32 {
        return Err(Error::RustError(
            "SESSION_SECRET must be at least 32 characters".to_owned(),
        ));
    }
    Ok(secret.into_bytes())
}

/// The session cookie's user, if the cookie is valid.
fn session_user(req: &Request, secret: &[u8]) -> Result<Option<String>> {
    let cookies = req.headers().get("cookie")?.unwrap_or_default();
    Ok(cookies
        .split(';')
        .find_map(|cookie| cookie.trim().strip_prefix(&format!("{SESSION_COOKIE}=")))
        .and_then(|value| token::verify(secret, Kind::Session, value, now())))
}

/// A `Set-Cookie` value. `Secure` only over HTTPS, so local runs over HTTP work.
fn session_cookie(req: &Request, value: &str, max_age: u64) -> Result<String> {
    let secure = if req.url()?.scheme() == "https" {
        "; Secure"
    } else {
        ""
    };
    Ok(format!(
        "{SESSION_COOKIE}={value}; Path=/; HttpOnly; SameSite=Strict; Max-Age={max_age}{secure}"
    ))
}

fn access_token(secret: &[u8], user: String) -> AccessToken {
    let expires_at = now() + ACCESS_TTL_SECS;
    AccessToken {
        token: token::sign(secret, Kind::Access, &user, expires_at),
        user,
        expires_at,
    }
}

fn query_param(url: &Url, name: &str) -> Option<String> {
    url.query_pairs()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.into_owned())
}

/// The `{room}` route parameter, percent-decoded.
fn route_room<D>(ctx: &RouteContext<D>) -> Option<String> {
    ctx.param("room").and_then(|room| decode_segment(room))
}

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    Router::new()
        .get_async("/partyline/chat/:room", |req, ctx| async move {
            if route_room(&ctx).as_deref() != Some(ROOM) {
                return reject(close::BAD_REQUEST, "no such room");
            }
            // Only the access token opens a socket. The browser also sends the session
            // cookie with the upgrade, but checking it here would skip the token provider.
            let secret = secret(&ctx.env)?;
            let Some(user) = query_param(&req.url()?, "token")
                .and_then(|token| token::verify(&secret, Kind::Access, &token, now()))
            else {
                return reject(close::UNAUTHORIZED, "token missing or expired");
            };
            // The tag lets sign-out close this user's sockets.
            Connect::<Chat>::new(ROOM)
                .tag(user)
                .forward(&ctx.env, BINDING, req)
                .await
        })
        .post_async("/api/login", |mut req, ctx| async move {
            // A stand-in for an identity provider: anyone can sign in as any name.
            let Ok(Login { name }) = req.json().await else {
                return Response::error("expected a name", 400);
            };
            let name = name.trim().to_owned();
            if name.is_empty()
                || name.chars().count() > MAX_NAME_LEN
                || name.chars().any(char::is_control)
            {
                return Response::error("invalid name", 400);
            }
            let secret = secret(&ctx.env)?;
            let session = token::sign(&secret, Kind::Session, &name, now() + SESSION_TTL_SECS);
            let cookie = session_cookie(&req, &session, SESSION_TTL_SECS)?;
            let response = Response::from_json(&access_token(&secret, name))?;
            response.headers().set("set-cookie", &cookie)?;
            Ok(response)
        })
        .get_async("/api/token", |req, ctx| async move {
            let secret = secret(&ctx.env)?;
            match session_user(&req, &secret)? {
                Some(user) => Response::from_json(&access_token(&secret, user)),
                None => Response::error("not signed in", 401),
            }
        })
        .post_async("/api/logout", |req, ctx| async move {
            let secret = secret(&ctx.env)?;
            if let Some(user) = session_user(&req, &secret)? {
                // 4403 tells every tab of this user to stop, without reconnecting.
                Publisher::<Chat>::new(&ctx.env, BINDING)?
                    .close_tagged(ROOM, &user, close::FORBIDDEN)
                    .await?;
            }
            let response = Response::empty()?.with_status(204);
            response
                .headers()
                .set("set-cookie", &session_cookie(&req, "", 0)?)?;
            Ok(response)
        })
        .get_async("/api/head", |_req, ctx| async move {
            let head = Publisher::<Chat>::new(&ctx.env, BINDING)?
                .head(ROOM)
                .await?;
            Response::from_json(&head)
        })
        .post_async("/api/messages", |mut req, ctx| async move {
            let secret = secret(&ctx.env)?;
            let Some(author) = session_user(&req, &secret)? else {
                return Response::error("not signed in", 401);
            };
            let Ok(PostMessage { text }) = req.json().await else {
                return Response::error("expected a message", 400);
            };
            let text = text.trim().to_owned();
            if text.is_empty() || text.chars().count() > MAX_MESSAGE_LEN {
                return Response::error("invalid message", 400);
            }
            let head = Publisher::<Chat>::new(&ctx.env, BINDING)?
                .publish(ROOM, &ChatEvent::MessagePosted { author, text })
                .await?;
            Response::from_json(&head)
        })
        .run(req, env)
        .await
}
