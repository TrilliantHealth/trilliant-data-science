Files in this directory should have no imports from anything inside mops, or anything in any other part of mops with the exception of `config`, including other utils.

Each file should therefore be a completely self-contained "system". Anything requiring more than 200 lines should be moved to submodule and split up inside there.
