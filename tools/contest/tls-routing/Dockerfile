FROM nginx:1.24.0
RUN printf 'deb https://deb.debian.org/debian bullseye main\n' > /etc/apt/sources.list && apt-get update && apt-get install -y --no-install-recommends nftables iproute2 procps systemd && rm -rf /var/lib/apt/lists/*
# Match Ubuntu's merged /usr executable path used by the deployed unit.
RUN ln -s /sbin/sysctl /usr/sbin/sysctl
