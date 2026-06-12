#!/bin/sh
# Install the omniagent CLI from the latest GitHub nightly release.
#
# Re-running this script upgrades an existing install in place: it pulls the
# latest nightly and overwrites the binary, reporting the version change.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/brian14708/omniagent/main/install.sh | sh
#
# Overrides (environment variables):
#   INSTALL_DIR=/usr/local/bin   where to put the binary (default: ~/.local/bin)
#   OMNIAGENT_VERSION=nightly    release tag to install (default: nightly)
#
# Supported platforms: Linux x86_64, Linux aarch64, macOS (Apple Silicon).

set -eu

REPO="brian14708/omniagent"
TAG="${OMNIAGENT_VERSION:-nightly}"
INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"
BIN_NAME="omniagent"

err() {
	echo "omniagent install: $*" >&2
	exit 1
}

info() {
	echo "omniagent install: $*"
}

# --- Detect platform ----------------------------------------------------------
os="$(uname -s)"
arch="$(uname -m)"

case "$arch" in
	x86_64 | amd64) arch="x86_64" ;;
	aarch64 | arm64) arch="aarch64" ;;
esac

case "$os" in
	Linux) os="linux" ;;
	Darwin) os="darwin" ;;
	*) os="$os" ;;
esac

case "${os}-${arch}" in
	linux-x86_64) bundle="linux-x86_64" ;;
	linux-aarch64) bundle="linux-aarch64" ;;
	darwin-aarch64) bundle="darwin-aarch64" ;;
	*)
		err "unsupported platform '${os}-${arch}'. Nightly builds are published only for: linux-x86_64, linux-aarch64, darwin-aarch64 (Apple Silicon)."
		;;
esac

# --- Pick a downloader --------------------------------------------------------
if command -v curl >/dev/null 2>&1; then
	download() { curl -fsSL "$1" -o "$2"; }
elif command -v wget >/dev/null 2>&1; then
	download() { wget -qO "$2" "$1"; }
else
	err "need curl or wget to download release assets."
fi

asset="omniagent-cli-nightly-${bundle}.tar.gz"
base_url="https://github.com/${REPO}/releases/download/${TAG}"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT INT TERM

# --- Download -----------------------------------------------------------------
info "downloading ${asset} from ${TAG} release..."
download "${base_url}/${asset}" "${tmp}/${asset}" ||
	err "failed to download ${base_url}/${asset}"

# --- Verify checksum ----------------------------------------------------------
if download "${base_url}/SHA256SUMS" "${tmp}/SHA256SUMS" 2>/dev/null; then
	expected="$(grep "$asset" "${tmp}/SHA256SUMS" | awk '{print $1}' | head -n1)"
	if [ -z "$expected" ]; then
		info "warning: ${asset} not found in SHA256SUMS; skipping checksum verification."
	else
		if command -v sha256sum >/dev/null 2>&1; then
			actual="$(sha256sum "${tmp}/${asset}" | awk '{print $1}')"
		elif command -v shasum >/dev/null 2>&1; then
			actual="$(shasum -a 256 "${tmp}/${asset}" | awk '{print $1}')"
		else
			actual=""
			info "warning: no sha256sum/shasum tool found; skipping checksum verification."
		fi
		if [ -n "$actual" ] && [ "$actual" != "$expected" ]; then
			err "checksum mismatch for ${asset} (expected ${expected}, got ${actual})."
		fi
	fi
else
	info "warning: could not download SHA256SUMS; skipping checksum verification."
fi

# --- Extract & install --------------------------------------------------------
tar -xzf "${tmp}/${asset}" -C "$tmp" ||
	err "failed to extract ${asset}."
[ -f "${tmp}/${BIN_NAME}" ] ||
	err "archive did not contain expected binary '${BIN_NAME}'."

# Capture the currently installed version (if any) so we can report the change.
old_ver=""
if [ -x "${INSTALL_DIR}/${BIN_NAME}" ]; then
	old_ver="$("${INSTALL_DIR}/${BIN_NAME}" --version 2>/dev/null || true)"
fi

mkdir -p "$INSTALL_DIR"
mv "${tmp}/${BIN_NAME}" "${INSTALL_DIR}/${BIN_NAME}"
chmod +x "${INSTALL_DIR}/${BIN_NAME}"

new_ver="$("${INSTALL_DIR}/${BIN_NAME}" --version 2>/dev/null || true)"
if [ -n "$old_ver" ] && [ "$old_ver" != "$new_ver" ]; then
	info "upgraded: ${old_ver} -> ${new_ver}"
elif [ -n "$old_ver" ]; then
	info "already up to date: ${new_ver:-$BIN_NAME}"
else
	info "installed ${BIN_NAME} to ${INSTALL_DIR}/${BIN_NAME} (${new_ver:-unknown version})"
fi

# --- PATH hint ----------------------------------------------------------------
case ":${PATH}:" in
	*":${INSTALL_DIR}:"*) ;;
	*)
		echo
		info "${INSTALL_DIR} is not on your PATH. Add it with:"
		echo "    export PATH=\"${INSTALL_DIR}:\$PATH\""
		;;
esac

# --- Next steps ---------------------------------------------------------------
cat <<'EOF'

Next steps:
  1. Authenticate against your OmniAgent console:
       omniagent login --server-url https://your-console.example --token <API token>
  2. Allow a workspace the daemon may run agents under:
       omniagent workspaces add /path/to/your/project
  3. Start the daemon (foreground; connects out to the console):
       omniagent daemon
  Then start a session from the OmniAgent web console.
EOF
