# Kubernetes

```bash
kubectl apply -k deploy/k8s/base          # dev/minikube
kubectl apply -k deploy/k8s/overlays/prod # prod replica counts + public host patches
```

## Production checklist

1. **Secrets** — replace `arena-admin` (`ADMIN_TOKEN`, `JWT_SECRET`, `REDIS_PASSWORD`)
2. **TLS** — install [cert-manager](https://cert-manager.io/) + nginx ingress; edit host in `overlays/prod/patch-ingress-host.yaml`
3. **Redis** — bundled StatefulSet has AOF + password; use managed Redis/Sentinel at scale
4. **Kafka** — set `KAFKA_BROKERS` to managed cluster (optional)
5. **GameServer public routing** — clients need WSS to GS pods; use Agones/public LB per pod in real prod

## Auth flow (JWT enabled)

```bash
curl -X POST https://arena.example.com/auth/login -d '{"name":"pilot"}'
# -> {"token":"...","player_id":"p-...","expires_at":...}
# WebSocket ?id=<player_id> + hello.access_token
```

## Ops

- Dynamic config: `PUT /admin/config` with `Authorization: Bearer $ADMIN_TOKEN`
- Metrics: `/metrics` (Prometheus)
- Drain: SIGTERM → stop new matches → wait `DRAIN_TIMEOUT` → close rooms
