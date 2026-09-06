# Git & GitHub Setup



---

## Step 1 — Install Git

### Windows

1. Download Git for Windows: https://git-scm.com/download/win
2. Run the installer
3. Important options to select:
   - **Editor:** Use Visual Studio Code (or your preferred editor)
   - **PATH:** Git from the command line and 3rd-party software ✅
   - **HTTPS transport:** Use the OpenSSL library
   - **Line ending conversions:** Checkout Windows-style, commit Unix-style
   - **Terminal emulator:** Use MinTTY
   - **Credential Manager:** Git Credential Manager Core

### Mac

```bash
# Option A — Homebrew (recommended)
brew install git

# Option B — Xcode Command Line Tools
xcode-select --install
```

### Linux

```bash
sudo apt update
sudo apt install git
```

### Verify

```bash
git --version
# Should show 2.x or higher
```

---

## Step 2 — Configure Git

### Set your identity

These appear on every commit you make.

```bash
git config --global user.name "Your Full Name"
git config --global user.email "your.email@company.com"
```

> Use the same email associated with your GitHub account.

### Set default branch name

```bash
git config --global init.defaultBranch main
```

### Set default editor (optional)

```bash
# VS Code
git config --global core.editor "code --wait"

# Nano
git config --global core.editor "nano"
```

### Better diff colours

```bash
git config --global color.ui auto
```

### Verify config

```bash
git config --list
```

---

## Step 3 — Set up GitHub authentication

You'll be prompted for credentials when you push code. **GitHub no longer accepts passwords** — you need a Personal Access Token (PAT) or SSH key.

### Option A — Personal Access Token (easier)

1. Go to GitHub → **Settings** (click your avatar, top right)
2. **Developer settings** (bottom of left sidebar)
3. **Personal access tokens → Tokens (classic)**
4. **Generate new token (classic)**
5. Configure:
   - **Note:** "Assessment work"
   - **Expiration:** 90 days
   - **Scopes:** Tick `repo` (gives full repo access)
6. **Generate token** at the bottom
7. **Copy the token now** — you won't be able to see it again

When Git asks for a password during `git push`, paste the token instead.

### Save the token (so you don't have to paste every time)

**Mac:**
```bash
git config --global credential.helper osxkeychain
```

**Windows:**
Already handled by Git Credential Manager if you installed Git for Windows.

**Linux:**
```bash
git config --global credential.helper "cache --timeout=3600"
```

### Option B — SSH key (more secure)

1. Generate a key:
   ```bash
   ssh-keygen -t ed25519 -C "your.email@company.com"
   ```
   Accept defaults, optionally set a passphrase.

2. Copy the public key:
   ```bash
   # Mac
   pbcopy < ~/.ssh/id_ed25519.pub

   # Windows (Git Bash)
   cat ~/.ssh/id_ed25519.pub | clip

   # Linux
   cat ~/.ssh/id_ed25519.pub
   ```

3. Add to GitHub: **Settings → SSH and GPG keys → New SSH key** → paste

4. Test:
   ```bash
   ssh -T git@github.com
   ```

5. When cloning, use the SSH URL (starts with `git@github.com:`) instead of HTTPS.

---

