"""
SOFE 4630U Milestone 2 - Design part: verify the Redis storage stage.

Connects to the Redis server deployed on GKE and reports the records that the
Redis sink connector stored, which is the check step for the Redis pipeline.

    python3 verify_redis.py <redis-external-ip>
"""

import json
import sys

try:
    import redis   # pip install redis
except ModuleNotFoundError:
    raise SystemExit("The redis library is required.  Install it with:  pip install redis")

# ======= EDIT THIS =======
REDIS_PASSWORD = "sofe4630u"   # set in redis.yaml
REDIS_PORT = 6379
REDIS_DB = 0
KEY_PREFIX = "reading:"
# =========================


def main():
    if len(sys.argv) < 2:
        raise SystemExit(
            "Usage: python3 verify_redis.py <redis-external-ip>\n"
            "Get the IP with: kubectl get services"
        )
    host = sys.argv[1]

    print(f"Connecting to Redis at {host}:{REDIS_PORT} ...")
    r = redis.Redis(host=host, port=REDIS_PORT, db=REDIS_DB,
                    password=REDIS_PASSWORD, decode_responses=True)
    r.ping()

    keys = sorted(r.keys(f"{KEY_PREFIX}*"))
    print(f"\nKeys matching '{KEY_PREFIX}*': {len(keys)}\n")

    if not keys:
        print("No keys found. Check that the integration is published")
        print("and that the connector is not suspended.")
        return

    # count the keys belonging to each profile
    profiles = {}
    for k in keys:
        parts = k.split(":")
        if len(parts) >= 2:
            profiles[parts[1]] = profiles.get(parts[1], 0) + 1

    print("Keys per profile:")
    for name, n in sorted(profiles.items(), key=lambda kv: -kv[1]):
        print(f"  {name:<10} {n}")

    print("\nFirst 3 stored values:")
    for k in keys[:3]:
        raw = r.get(k)
        try:
            value = json.loads(raw)
            print(f"  {k}")
            for field, v in value.items():
                print(f"      {field}: {v}")
        except (TypeError, ValueError):
            # the value is not JSON, print it as stored
            print(f"  {k} -> {raw}")


if __name__ == "__main__":
    main()
