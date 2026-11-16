# Suwayomi Multi-Extension Gateway & Stream Bridge (`suwayomi-bridge`)

![Python](https://img.shields.io/badge/Python-3.12+-3776AB?style=flat-square&logo=python&logoColor=white)
![Node.js](https://img.shields.io/badge/Node.js-22+-339933?style=flat-square&logo=nodedotjs&logoColor=white)
![Kotlin](https://img.shields.io/badge/Kotlin-Suwayomi%20%2F%20Tachiyomi-7F52FF?style=flat-square&logo=kotlin&logoColor=white)
![Android](https://img.shields.io/badge/Android-ARM64%20Daemon-3DDC84?style=flat-square&logo=android&logoColor=white)

Repositório da infraestrutura de proxy reverso e resolução de desafios de rede para **Suwayomi Server**. Projetado para permitir que extensões executando em servidores VPS/cloud utilizem túneis residenciais reversos transparentes para contornar desafios de bot-protection, Web Workers e descriptografia em tempo real.

---

## 1. Visão Geral da Arquitetura

Instâncias auto-hospedadas do Suwayomi em servidores VPS frequentemente enfrentam bloqueios por IP de datacenter e desafios criptográficos baseados em JavaScript/Wasm executados no navegador.

A arquitetura resolve isso separando as responsabilidades em **3 camadas**:

```text
┌─────────────────────────────────────────────────────────────────────────┐
│ VPS (<USUARIO_VPS>@<IP_DA_VPS>)                                         │
│                                                                         │
│  ┌──────────────────────┐        HTTP (127.0.0.1:8192)                  │
│  │ Container: suwayomi  │ ───────────────────────────────────┐          │
│  │ (Extensões Suwayomi) │                                    ▼          │
│  └──────────────────────┘                         ┌───────────────────┐ │
│    (network_mode: service:warp)                   │ Container:        │ │
│                                                   │ gateway-proxy     │ │
│                                                   │ (gateway.py)      │ │
│                                                   └─────────┬─────────┘ │
│                                                             │           │
│                                      Encaminha /* para      │           │
│                                      172.19.0.1:18192       ▼           │
│                                                  ┌────────────────────┐ │
│                                                  │ Túnel SSH Reverso  │ │
│                                                  │ (Porta 18192)      │ │
│                                                  └──────────┬─────────┘ │
└─────────────────────────────────────────────────────────────┼───────────┘
                                                              │
                                         SSH sob Internet     │
                                                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ Host Residencial (Celular Android via Wi-Fi/ADB ou PC Local)            │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ proxy_service.py (127.0.0.1:8192)                                 │  │
│  │  ├─ Endpoints Universais (/api, /proxy, /image, /health)          │  │
│  │  ├─ Worker Bridge Node.js (worker_bridge.js)                      │  │
│  │  └─ Plugins Dinâmicos c/ Hot-Reload (extensions/*.py)             │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

### Componentes Principais

1. **Gateway Coringa na VPS (`gateway/gateway.py`):**
   - Roda em container Docker na VPS (`0.0.0.0:8192`, compartilhando a rede com o container `suwayomi`).
   - Proxy reverso HTTP 100% agnóstico e sem dependências externas: repassa qualquer método (`GET`, `POST`, `PUT`, `DELETE`, etc.), cabeçalhos e corpo para o túnel reverso (`172.19.0.1:18192`).
   - Não requer alterações para adicionar ou modificar extensões.

2. **Proxy Residencial & Worker Bridge (`proxy_service.py` + `worker_bridge.js`):**
   - Executa no host residencial (Android ARM64 ou Linux x86_64) com TLS fingerprint de navegador real (`curl_cffi`).
   - Sistema de **plugins dinâmicos** na pasta `extensions/`: novos manipuladores com hot-reload automático por `mtime`.

---

## 2. Estrutura do Repositório

```text
suwayomi-bridge/
├── README.md                          # Documentação principal
├── AGENTS.md                          # Guia técnico de contexto e arquitetura
├── .env.example                       # Modelo de variáveis de ambiente
├── proxy_service.py                   # Servidor proxy HTTP multi-extensões
├── worker_bridge.js                   # Bridge Node.js para resolução de desafios criptográficos
├── extensions/                        # Diretório de plugins dinâmicos
│   └── _example.py                    # Template de exemplo para novos plugins
├── gateway/                           # Código do Gateway Coringa para a VPS
│   ├── Dockerfile
│   └── gateway.py
├── phone_daemon.sh                    # Script daemon/watchdog para Android (ARM64)
├── phone_watchdog.sh                  # Serviço watchdog no PC para reconexão ADB
├── Dockerfile.builder                 # Ambiente Docker auxiliar para build de extensões
└── run-docker-build.sh                # Script auxiliar de compilação
```

---

## 3. Endpoints Universais

Disponíveis para qualquer extensão Suwayomi:

- `GET/POST/PUT/DELETE/PATCH/HEAD /api?url=<URL_ENCODED>` (ou `/proxy?url=<URL_ENCODED>`):
  - Deduz automaticamente `Referer` e `Origin` com base no domínio de destino.
  - Parâmetros opcionais: `&referer=...`, `&origin=...`, `&ua=...`, `&cookie=...`, `&method=...`, `&no_cache=1`.
  - Repassa cabeçalhos enviados com prefixo `X-Proxy-Header-<Nome>` ou `X-Forward-<Nome>`.
  - Retorna o cabeçalho `X-Target-Url` com a URL final após redirecionamentos.
- `GET /image?url=<URL_ENCODED>`:
  - Cache local de imagens em disco, dedução automática de cabeçalhos de referência e detecção de tipo MIME.
- `GET /health`:
  - Retorna `200 OK` (`OK`) para verificação de integridade do serviço.

---

## 4. Configuração das Variáveis de Ambiente (`.env`)

Copie o modelo de configuração:
```bash
cp .env.example .env
```

Edite o arquivo `.env` com os dados da sua infraestrutura:
```ini
VPS_USER=usuario_ssh
VPS_HOST=seu_ip_vps
VPS_SSH_KEY=/data/local/tmp/suwayomi-proxy/ssh-key.key
VPS_DOCKER_BRIDGE=172.19.0.1
VPS_TUNNEL_PORT=18192
VPS_WARP_IP=172.19.0.5
VPS_WARP_PORT=1080
```

---

## 5. Operação e Monitoramento

### 5.1. No Host Conectado (via ADB / Android)

- **Verificar status dos processos e health check:**
  ```bash
  adb shell "/data/local/tmp/suwayomi-proxy/phone_daemon.sh status"
  ```
- **Ver logs em tempo real:**
  ```bash
  adb shell "/data/local/tmp/suwayomi-proxy/phone_daemon.sh logs"
  ```
- **Reiniciar serviços:**
  ```bash
  adb shell "/data/local/tmp/suwayomi-proxy/phone_daemon.sh restart"
  ```

### 5.2. Teste Fim-a-Fim a partir da VPS

```bash
ssh -i /caminho/chave.key <USUARIO>@<IP_VPS> \
  'docker exec suwayomi curl -fsS http://127.0.0.1:8192/health && echo ""'
```

---

## 6. Criação de Novos Plugins

Para adicionar manipulação customizada a uma nova fonte:

1. Crie um arquivo `extensions/meu_plugin.py` baseado no template `extensions/_example.py`:
   ```python
   def handle_request(handler, method: str, parsed, qs: dict) -> bool:
       if not parsed.path.startswith("/meu_plugin/"):
           return False
       # Implementação customizada
       return True
   ```
2. O serviço recarrega o plugin automaticamente em tempo de execução.
