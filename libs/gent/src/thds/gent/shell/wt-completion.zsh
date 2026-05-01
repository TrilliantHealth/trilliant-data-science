#!/usr/bin/env zsh
# Zsh completion for wt command

# Helper function to get worktrees with descriptions
_wt_get_worktrees() {
    local -a worktrees
    local line
    while IFS=$'\t' read -r name desc; do
        worktrees+=("$name:$desc")
    done < <(wt-completion --worktrees --with-descriptions 2>/dev/null)
    _describe 'worktree' worktrees
}

# Helper function to get worktrees without descriptions (for simpler contexts)
_wt_get_worktrees_simple() {
    local -a worktrees
    worktrees=(${(f)"$(wt-completion --worktrees 2>/dev/null)"})
    compadd -a worktrees
}

# Helper function to get branches
_wt_get_branches() {
    local -a branches
    branches=(${(f)"$(wt-completion --branches 2>/dev/null)"})
    compadd -a branches
}

# Helper function to get branches and worktrees (for co/add commands)
_wt_get_branches_and_worktrees() {
    local -a items
    items=(${(f)"$(wt-completion --branches 2>/dev/null)"})
    items+=(${(f)"$(wt-completion --worktrees 2>/dev/null)"})
    compadd -a items
}

# Main completion function for 'wt' command
_wt() {
    local -a subcommands
    local line

    # Get subcommands with descriptions
    while IFS=$'\t' read -r cmd desc; do
        subcommands+=("$cmd:$desc")
    done < <(wt-completion --subcommands --with-descriptions 2>/dev/null)

    _arguments -C \
        '1: :->subcommand' \
        '*:: :->args'

    case $state in
        subcommand)
            _describe 'wt subcommand' subcommands
            ;;
        args)
            case ${words[1]} in
                cd|init|start|path)
                    _wt_get_worktrees
                    ;;
                rm)
                    _arguments \
                        '(-f --force)'{-f,--force}'[Force removal of worktree with uncommitted changes]' \
                        '1:worktree:->worktree'

                    if [[ $state == worktree ]]; then
                        _wt_get_worktrees
                    fi
                    ;;
                co|add)
                    if [[ $CURRENT -eq 2 ]]; then
                        _wt_get_branches_and_worktrees
                    elif [[ $CURRENT -eq 3 ]]; then
                        _wt_get_branches
                    fi
                    ;;
                list|root)
                    # No completion for these commands
                    ;;
            esac
            ;;
    esac
}

# Register completion function
# Check if completion system is loaded and compdef is available
if (( $+functions[compdef] )) && [[ -n ${_comps+x} ]]; then
    # Unregister any existing completion first to avoid conflicts
    compdef -d wt 2>/dev/null
    compdef _wt wt
elif [[ -n $ZSH_VERSION ]]; then
    # Completion system not loaded, try to initialize it
    autoload -Uz compinit
    compinit -i
    if (( $+functions[compdef] )); then
        compdef _wt wt
    fi
fi
