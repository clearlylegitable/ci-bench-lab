set -euo pipefail

MINUTES="${MINUTES:-60}"
OS="$(uname -s)"
ARCH="$(uname -m)"
W="$HOME/sbx"
mkdir -p "$W"
cp sandbox/authorized_key.pub "$W/authorized_keys"
chmod 600 "$W/authorized_keys"
cd "$W"

echo "== machine"
SPECS="os=$OS arch=$ARCH"
echo "os=$OS arch=$ARCH user=$(whoami)"
if [ "$OS" = "Linux" ]; then
  echo "cpus=$(nproc) mem_mb=$(free -m | awk '/Mem:/{print $2}')"
  lscpu | grep -E 'Model name|^CPU\(s\)' || true
else
  echo "cpus=$(sysctl -n hw.ncpu) mem_mb=$(( $(sysctl -n hw.memsize) / 1048576 ))"
  sysctl -n machdep.cpu.brand_string || true
  sw_vers
  xcodebuild -version | head -2 || true
fi
df -h / | tail -1
if [ "$OS" = "Linux" ]; then SPECS="$SPECS cpus=$(nproc) mem_mb=$(free -m | awk '/Mem:/{print $2}')"; else SPECS="$SPECS cpus=$(sysctl -n hw.ncpu) mem_mb=$(( $(sysctl -n hw.memsize) / 1048576 ))"; fi

case "$OS-$ARCH" in
  Linux-x86_64) WS=linux_amd64; CF=cloudflared-linux-amd64 ;;
  Linux-aarch64) WS=linux_arm64; CF=cloudflared-linux-arm64 ;;
  Darwin-arm64) WS=darwin_arm64; CF=cloudflared-darwin-arm64.tgz ;;
  Darwin-x86_64) WS=darwin_amd64; CF=cloudflared-darwin-amd64.tgz ;;
  *) echo "unsupported $OS-$ARCH"; exit 1 ;;
esac

echo "== tools"
REL="$(curl -fsSL -H "Authorization: Bearer $GH_TOKEN" https://api.github.com/repos/erebe/wstunnel/releases/latest)"
WSURL="$(printf '%s' "$REL" | python3 -c "import json,sys;r=json.load(sys.stdin);print([a['browser_download_url'] for a in r['assets'] if a['name'].endswith('_${WS}.tar.gz')][0])")"
curl -fsSL "$WSURL" -o ws.tgz
tar xzf ws.tgz wstunnel
chmod +x wstunnel
curl -fsSL "https://github.com/cloudflare/cloudflared/releases/latest/download/$CF" -o "$CF"
case "$CF" in
  *.tgz) tar xzf "$CF" cloudflared ;;
  *) mv "$CF" cloudflared ;;
esac
chmod +x cloudflared

echo "== sshd"
if [ ! -x /usr/sbin/sshd ]; then
  sudo apt-get update -qq
  sudo apt-get install -y -qq openssh-server
fi
sudo mkdir -p /run/sshd /var/empty
ssh-keygen -q -t ed25519 -N '' -f "$W/host_key"
cat > "$W/sshd_config" <<EOF
Port 2222
ListenAddress 127.0.0.1
HostKey $W/host_key
AuthorizedKeysFile $W/authorized_keys
PasswordAuthentication no
KbdInteractiveAuthentication no
PubkeyAuthentication yes
UsePAM no
StrictModes no
PidFile $W/sshd.pid
Subsystem sftp internal-sftp
EOF
sudo /usr/sbin/sshd -f "$W/sshd_config"
sleep 1
(exec 3<>/dev/tcp/127.0.0.1/2222 && head -c 20 <&3 && echo) || { echo "sshd not listening"; exit 1; }

echo "== tunnel"
nohup ./wstunnel server --restrict-to 127.0.0.1:2222 ws://127.0.0.1:8080 > ws.log 2>&1 &
nohup ./cloudflared tunnel --no-autoupdate --url http://127.0.0.1:8080 > cf.log 2>&1 &

HOST=""
for i in $(seq 1 60); do
  HOST="$(grep -oE '[a-z0-9-]+\.trycloudflare\.com' cf.log | head -1 || true)"
  [ -n "$HOST" ] && break
  sleep 2
done
if [ -z "$HOST" ]; then
  echo "no tunnel url"
  tail -n 30 cf.log
  exit 1
fi

echo "SBX_HOST=$HOST"
echo "::notice title=sbx::host=$HOST user=$(whoami) $SPECS"
echo "SBX_USER=$(whoami)"
echo "SBX_STOP_FILE=$W/stop"

END=$(( $(date +%s) + MINUTES * 60 ))
while [ "$(date +%s)" -lt "$END" ] && [ ! -f "$W/stop" ]; do
  sleep 10
done
echo "== done"
