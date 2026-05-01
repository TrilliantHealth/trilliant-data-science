#!/bin/sh
# Git worktree management utilities (gent - Git Ent)
# Thin shell wrappers around wt CLI

# Main wt command - unified interface for all worktree operations
# Usage: wt <command> [options]
#
# All logic is inlined into this single function so that environments which
# selectively import shell functions (e.g. snapshot-based shells) get a
# fully self-contained wt() without needing sibling helper functions.
wt() {
    # --- version check (once per session) ---
    if [ -z "$_WT_VERSION_CHECKED" ]; then
        export _WT_VERSION_CHECKED=1

        local installed_version=""
        [ -f ~/.gent/version ] && installed_version=$(cat ~/.gent/version 2>/dev/null)

        local package_version=""
        local wt_bin=""
        for candidate in "$HOME/.local/bin/wt" "$(command -v wt 2>/dev/null)"; do
            [ -f "$candidate" ] && wt_bin="$candidate" && break
        done
        if [ -n "$wt_bin" ]; then
            local wt_python
            wt_python=$(head -n 1 "$wt_bin" | sed 's/^#!//')
            [ -x "$wt_python" ] && package_version=$("$wt_python" -c 'import thds.gent; print(thds.gent.SHELL_INTEGRATION_VERSION)' 2>/dev/null)
        fi

        if [ -n "$package_version" ] && [ "$installed_version" != "$package_version" ]; then
            echo "" >&2
            echo "⚠️  wt shell integration is outdated (v${installed_version:-missing} → v$package_version)" >&2
            echo "   Run: wt setup-shell" >&2
            echo "" >&2
        fi
    fi

    if [ -z "$1" ]; then
        command wt --help
        return 1
    fi

    local cmd="$1"
    shift

    # Export marker so Python knows it's running through the shell wrapper
    export _WT_SHELL_WRAPPER=1

    case "$cmd" in
        root|cd|co)
            # Navigation commands - capture path from last line while streaming output
            local target
            # Use tee to stream output if TTY available, otherwise just capture
            if [ -t 1 ] && [ -w /dev/tty ]; then
                target=$(command wt "$cmd" "$@" | tee /dev/tty | tail -1)
            else
                target=$(command wt "$cmd" "$@" | tail -1)
            fi
            local exit_code=$?

            # Check for installation issues
            if [ $exit_code -ne 0 ] && echo "$target" | grep -q "ModuleNotFoundError.*thds"; then
                echo "⚠️  wt installation issue detected" >&2
                echo "The editable install may be pointing to a moved or deleted location." >&2
                echo "" >&2
                echo "To fix, reinstall from your main worktree:" >&2
                echo "  wt setup-shell" >&2
                return 1
            fi

            if [ $exit_code -eq 0 ] && [ -d "$target" ]; then
                cd "$target" || return 1
            else
                return $exit_code
            fi
            ;;
        *)
            # All other commands - stream output directly
            command wt "$cmd" "$@"
            ;;
    esac
}

# Load shell-specific completions from ~/.gent/
if [ -n "$BASH_VERSION" ] && [ -f ~/.gent/wt-completion.bash ]; then
    source ~/.gent/wt-completion.bash
elif [ -n "$ZSH_VERSION" ] && [ -f ~/.gent/wt-completion.zsh ]; then
    source ~/.gent/wt-completion.zsh
fi
