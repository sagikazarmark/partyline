# Start a Worker under wrangler dev in the background and stop it on exit.
# Source it from a bash script, then call `serve`.

# serve DIR PORT: run wrangler dev in DIR on PORT and wait until it answers HTTP,
# or fail with its log if it exits or does not answer in 120 seconds.
serve() {
    local dir=$1 port=$2 log pid i
    log=$(mktemp)
    (cd "$dir" && exec wrangler dev --port "$port" --inspector-port "$((port + 1000))") >"$log" 2>&1 &
    pid=$!
    trap "kill $pid 2>/dev/null || true; wait 2>/dev/null || true" EXIT

    for ((i = 0; i < 120; i++)); do
        if curl -s --max-time 2 -o /dev/null "http://localhost:$port/"; then
            echo "wrangler dev is up on http://localhost:$port"
            return
        fi
        kill -0 "$pid" 2>/dev/null || break
        sleep 1
    done

    echo "wrangler dev on port $port did not start:" >&2
    cat "$log" >&2
    return 1
}
