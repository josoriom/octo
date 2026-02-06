# Install `octo`

## 0) Choose install directory

`INSTALL_DIR` can be any directory on your machine, for example the folder where you usually keep your command-line programs (e.g. `~/.local/bin` or `/usr/local/bin`).

```bash
INSTALL_DIR="/path/to/your/directory"
mkdir -p "$INSTALL_DIR"
```

---

## 1) Download

### macOS — arm64

```bash
curl -fL "https://raw.githubusercontent.com/josoriom/octo/main/crates/cli/artifacts/macos-arm64/octo" -o "$INSTALL_DIR/octo"
chmod +x "$INSTALL_DIR/octo"
```

### macOS — x86_64

```bash
curl -fL "https://raw.githubusercontent.com/josoriom/octo/main/crates/cli/artifacts/macos-x86_64/octo" -o "$INSTALL_DIR/octo"
chmod +x "$INSTALL_DIR/octo"
```

### Linux — x86_64

```bash
curl -fL "https://raw.githubusercontent.com/josoriom/octo/main/crates/cli/artifacts/linux-x86_64/octo" -o "$INSTALL_DIR/octo"
chmod +x "$INSTALL_DIR/octo"
```

### Linux — arm64

```bash
curl -fL "https://raw.githubusercontent.com/josoriom/octo/main/crates/cli/artifacts/linux-arm64/octo" -o "$INSTALL_DIR/octo"
chmod +x "$INSTALL_DIR/octo"
```

---

## 2) Add `INSTALL_DIR` to `PATH` (shell environment)

This appends an `export PATH=...` line to your shell, so your `PATH` environment variable includes `INSTALL_DIR` and `octo` is runnable from any directory.
`source` reloads the shell file in the current terminal session (otherwise open a new terminal).

### macOS (zsh)

```bash
echo "export PATH=\"$INSTALL_DIR:\$PATH\"" >> "$HOME/.zshrc"
source "$HOME/.zshrc"
```

### Linux (bash)

```bash
echo "export PATH=\"$INSTALL_DIR:\$PATH\"" >> "$HOME/.bashrc"
source "$HOME/.bashrc"
```

---

## 3) Test

```bash
octo --help
```

## 4) [DOCS](./src/README.MD)
