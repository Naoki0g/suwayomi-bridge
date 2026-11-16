FROM ghcr.io/cirruslabs/android-sdk:34

USER root
RUN yes | sdkmanager "build-tools;36.0.0" "platforms;android-37.0"
RUN chmod -R 777 /opt/android-sdk-linux
