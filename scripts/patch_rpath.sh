#!/usr/bin/env zsh
set -euo pipefail

typeset -A visited

add_rpaths() {
  local target="$1"

  local existing
  existing=$(otool -l "$target" | awk '/LC_RPATH/{getline; getline; print $2}')

  for r in "@loader_path" "@loader_path/../lib"; do
    if ! print -l "$existing" | grep -qx "$r"; then
      echo "  Adding rpath $r to $target"
      install_name_tool -add_rpath "$r" "$target" 2>/dev/null || true
    fi
  done
}

fix_dylib() {
  local target="$1"

  # Prevent infinite recursion
  if [[ -n "${visited[$target]-}" ]]; then
    return
  fi
  visited[$target]=1

  echo "Inspecting: $target"

  local base
  base=$(basename "$target")

  echo "  Setting ID to @rpath/$base"
  install_name_tool -id "@rpath/$base" "$target" 2>/dev/null || true

  add_rpaths "$target"

  otool -L "$target" | tail -n +2 | awk '{print $1}' | while read dep; do
    # Skip system libs and already-correct ones
    if [[ "$dep" == @* ]] ||
       [[ "$dep" == /usr/lib/* ]] ||
       [[ "$dep" == /System/Library/* ]]; then
      continue
    fi

    local depbase
    depbase=$(basename "$dep")
    local new="@rpath/$depbase"

    echo "  Rewriting $dep -> $new"
    install_name_tool -change "$dep" "$new" "$target" 2>/dev/null || true

    # Recurse if local file exists
    if [[ -f "$depbase" ]]; then
      fix_dylib "$depbase"
    fi
  done
}

fix_dylib "$1"