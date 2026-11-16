#!/usr/bin/env bash
set -e

WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/build-workspace" && pwd)"
OUTPUT_APK="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/suwayomi-pt-source_a.apk"
OUTPUT_JAR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/suwayomi-pt-source_a.jar"

echo "=== Iniciando Build Docker no workspace: $WORKSPACE_DIR ==="

docker run --rm \
  -u "$(id -u):$(id -g)" \
  -v "$WORKSPACE_DIR:/workspace" \
  -w /workspace \
  -e GRADLE_USER_HOME=/workspace/.gradle \
  suwayomi-ext-builder:latest \
  bash -c "./gradlew :src:pt:source_a:assembleDebug :src:pt:source_a:assembleRelease --stacktrace"

echo "=== Localizando APK e JAR gerados ==="
GENERATED_APK=$(find "$WORKSPACE_DIR/src/pt/source_a/build/outputs/apk" -name "*.apk" | sort | tail -n 1)
GENERATED_JAR=$(find "$WORKSPACE_DIR/src/pt/source_a/build/outputs/jar" -name "*.jar" 2>/dev/null | sort | tail -n 1 || true)

if [ -f "$GENERATED_APK" ]; then
  cp "$GENERATED_APK" "$OUTPUT_APK"
  echo "=== APK copiado para: $OUTPUT_APK ==="
  ls -lh "$OUTPUT_APK"
fi

if [ -n "$GENERATED_JAR" ] && [ -f "$GENERATED_JAR" ]; then
  cp "$GENERATED_JAR" "$OUTPUT_JAR"
  echo "=== JAR copiado para: $OUTPUT_JAR ==="
  ls -lh "$OUTPUT_JAR"
fi
