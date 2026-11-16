# Suwayomi Multi-Extension Gateway & Stream Bridge — Contexto Técnico

Documento de contexto e especificações de arquitetura para agentes e mantenedores.

## Visão Geral da Arquitetura

O sistema atua como uma ponte de desvio residencial transparente para instâncias de Suwayomi Server rodando em nuvem/datacenter:

1. **Gateway Reverso na VPS (`gateway/gateway.py`):**
   - Executa no container Docker da VPS em modo host/bridge (`0.0.0.0:8192`).
   - Encaminha requisições de forma agnóstica para o endpoint upstream (`172.19.0.1:18192`).
   - Desacoplado de qualquer lógica de scraping ou extensão específica.

2. **Proxy Residencial (`proxy_service.py`):**
   - Executa no nó residencial (`127.0.0.1:8192`).
   - Fornece endpoints universais (`/api`, `/proxy`, `/image`, `/health`) e delega rotas customizadas para arquivos em `extensions/`.
   - Utiliza `curl_cffi` para emulação de TLS e headers de navegadores modernos.

3. **Daemon Android (`phone_daemon.sh`):**
   - Gerencia a execução em segundo plano do proxy Python e do túnel SSH reverso (`OpenSSH`).
   - Desativa mecanismos de economia agressiva de energia do sistema operacional para garantir disponibilidade permanente.

## Variáveis de Ambiente Suportadas

- `VPS_USER`: Usuário do SSH da VPS.
- `VPS_HOST`: Host/IP do servidor VPS.
- `VPS_SSH_KEY`: Caminho para a chave privada SSH.
- `VPS_TUNNEL_PORT`: Porta local da VPS mapeada para o proxy (padrão: `18192`).
- `VPS_DOCKER_BRIDGE`: Gateway IP da bridge Docker na VPS (padrão: `172.19.0.1`).
- `SOURCE_A_URL`: URL base configurável para fontes customizadas.
