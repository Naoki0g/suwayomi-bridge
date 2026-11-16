"""Exemplo de plugin para futura extensão customizada.

Qualquer arquivo `.py` (sem `_` no início do nome) colocado nesta pasta `extensions/`
é carregado automaticamente com hot-reload pelo `proxy_service.py` neste PC,
sem precisar reiniciar o proxy nem tocar na VPS.

Para extensões comuns que só precisam de bypass Cloudflare/TLS ou imagens com Referer,
não é necessário criar arquivo aqui: basta usar diretamente na extensão Kotlin:
  - http://127.0.0.1:8192/api?url=<url_codificada>
  - http://127.0.0.1:8192/proxy?url=<url_codificada>&referer=<opcional>
  - http://127.0.0.1:8192/image?url=<url_codificada>&referer=<opcional>
"""


def handle_request(handler, method: str, parsed, qs: dict) -> bool:
    if parsed.path == "/custom_ext_example":
        handler._send_json(200, {"ok": True, "method": method})
        return True
    return False
