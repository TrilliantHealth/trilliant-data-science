#!/usr/bin/env bash
# Bash completion for wt command

# Main completion function for 'wt' command
_wt_complete() {
    local cur prev words cword

    # Basic completion variable setup (no external dependencies)
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    words=("${COMP_WORDS[@]}")
    cword=$COMP_CWORD

    # words[0] is 'wt', words[1] is subcommand (if present)
    local subcommand="${words[1]}"

    # If we're completing the first argument, show subcommands
    if [[ $cword -eq 1 ]]; then
        COMPREPLY=($(compgen -W "$(wt-completion --subcommands 2>/dev/null)" -- "$cur"))
        return 0
    fi

    # Otherwise, completion depends on the subcommand
    case "$subcommand" in
        cd|init|start|path)
            # Complete with worktree names
            COMPREPLY=($(compgen -W "$(wt-completion --worktrees 2>/dev/null)" -- "$cur"))
            ;;
        rm)
            # Handle --force flag
            if [[ $cword -eq 2 && "$cur" == -* ]]; then
                COMPREPLY=($(compgen -W "--force -f" -- "$cur"))
            elif [[ $cword -eq 2 ]] || [[ $cword -eq 3 && ("${words[2]}" == "--force" || "${words[2]}" == "-f") ]]; then
                # Complete with worktree names (after optional --force flag)
                COMPREPLY=($(compgen -W "$(wt-completion --worktrees 2>/dev/null)" -- "$cur"))
            fi
            ;;
        co|add)
            # First arg: branches and worktrees
            # Second arg: base branch
            if [[ $cword -eq 2 ]]; then
                local branches worktrees
                branches=$(wt-completion --branches 2>/dev/null)
                worktrees=$(wt-completion --worktrees 2>/dev/null)
                COMPREPLY=($(compgen -W "$branches $worktrees" -- "$cur"))
            elif [[ $cword -eq 3 ]]; then
                # Base branch - just show branches
                COMPREPLY=($(compgen -W "$(wt-completion --branches 2>/dev/null)" -- "$cur"))
            fi
            ;;
        list|root)
            # No completion for these commands
            ;;
        *)
            # Unknown subcommand, no completion
            ;;
    esac
}

# Register completion function
complete -F _wt_complete wt
