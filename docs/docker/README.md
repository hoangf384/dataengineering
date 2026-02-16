# 🐳 Docker Commands – Frequently Used

## 🧹 Cleanup Docker System

```bash
# Remove unused containers, networks, images (including dangling & unused)
docker system prune -a -f
```

---

## 📦 List Containers & Images

```bash
# List all containers (running + stopped)
docker ps -a

# List all images
docker images
```

---

## 🔄 Restart Docker Compose Services

```bash
# Stop and remove containers, then start again in detached mode
docker compose down && docker compose up -d
```

---

## 🏗 Rebuild & Start Services

```bash
# Rebuild images and start containers
docker compose up --build -d
```

---

## 📜 View Container Logs

```bash
# Show last 10 lines of logs
docker logs --tail 10 <container-name>

# Follow logs in real-time
docker logs -f <container-name>
```

---

## 🖥 Access Container Shell

```bash
# Execute bash inside a running container
docker exec -it <container-name> bash
```
