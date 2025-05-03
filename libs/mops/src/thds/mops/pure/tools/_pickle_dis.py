import ast
import io
import pickletools
import re
import typing as ty

from thds.mops.pure.core import metadata, uris
from thds.mops.pure.core.memo import results
from thds.mops.pure.pickling._pickle import read_partial_pickle
from thds.mops.pure.runner import strings


def _dis(byts: bytes, indent: int = 4) -> str:
    """Disassemble the bytes into a string."""
    ios = io.StringIO()
    pickletools.dis(byts, out=ios, indentlevel=indent)
    ios.seek(0)
    return ios.read()


def replace_all_nested_pickles(
    disassembly_text: str,
) -> str:
    """
    Finds all BINBYTES opcodes whose payload starts with b'\\x80',
    attempts to disassemble them as pickles, and replaces the BINBYTES
    section with the nested disassembly if successful.
    """
    # Regex to find any BINBYTES line and capture indentation and start offset.
    binbytes_pattern = re.compile(
        r"^(?P<indent>[ \t]*)(?P<offset>\d+):\s+(?P<opcode>\S+)\s+BINBYTES\s+", re.MULTILINE
    )

    # Regex to find the start of the next opcode line after BINBYTES
    next_opcode_pattern = re.compile(r"^[ \t]*\d+:\s+\S+", re.MULTILINE)

    output_parts = []
    last_end = 0

    for match in binbytes_pattern.finditer(disassembly_text):
        indent_str = match.group("indent")
        binbytes_line_start = match.start()
        binbytes_line_end = match.end()  # End of the matched BINBYTES prefix

        # Find where the byte literal starts (b" or b') after the opcode
        bytes_literal_start_index = -1
        b_quote_match = re.search(r'b["\']', disassembly_text[binbytes_line_end:])
        if b_quote_match:
            bytes_literal_start_index = binbytes_line_end + b_quote_match.start()
        else:
            # Malformed BINBYTES line? Skip this match.
            # Append text up to the start of this BINBYTES line and continue searching
            output_parts.append(disassembly_text[last_end:binbytes_line_start])
            last_end = binbytes_line_start  # Start next search from here
            continue

        # Find the start of the *next* opcode line to delimit the byte literal
        next_opcode_match = next_opcode_pattern.search(disassembly_text, pos=binbytes_line_end)
        end_of_binbytes_section = (
            next_opcode_match.start() if next_opcode_match else len(disassembly_text)
        )

        # Extract the full string representation of the bytes literal
        potential_bytes_str = disassembly_text[
            bytes_literal_start_index:end_of_binbytes_section
        ].rstrip()

        nested_disassembly = None
        try:
            # Evaluate the string literal to get bytes
            actual_bytes = ast.literal_eval(potential_bytes_str)
            if not isinstance(actual_bytes, bytes):
                raise ValueError("Literal did not evaluate to bytes")

            # --- Key Check: Does it start with a pickle protocol marker? ---
            if actual_bytes.startswith(b"\x80"):
                # Attempt to disassemble these bytes
                indent_level = len(indent_str)
                # Use a deeper indent for the nested part
                nested_disassembly = _dis(actual_bytes, indent=indent_level + 4)

        except (SyntaxError, ValueError, TypeError):
            # Failed to parse the bytes literal string itself. Keep original.
            # print(f"Debug: Failed to eval bytes literal near offset {match.group('offset')}: {e_eval}")
            nested_disassembly = None  # Ensure it stays None
        except Exception:  # Catch errors from _dis (e.g., not valid pickle)
            # Failed to disassemble. Keep original.
            # print(f"Debug: Failed to disassemble potential pickle near offset {match.group('offset')}: {e_dis}")
            nested_disassembly = None  # Ensure it stays None

        # --- Construct the output ---
        # Append text before this BINBYTES line
        output_parts.append(disassembly_text[last_end:binbytes_line_start])

        if nested_disassembly:
            # Successfully disassembled, replace the BINBYTES section
            # Append the original BINBYTES line itself (for context)
            output_parts.append(
                disassembly_text[binbytes_line_start:binbytes_line_end]
            )  # Just the "XXX: B BINBYTES" part
            output_parts.append(f"--- NESTED PICKLE ({len(actual_bytes)} bytes) START ---\n")
            output_parts.append(nested_disassembly)
            output_parts.append(f"{indent_str}--- NESTED PICKLE END ---\n")
            # Update last_end to skip the original byte literal representation
            last_end = end_of_binbytes_section
        else:
            # Did not replace, append the original BINBYTES section unchanged
            output_parts.append(disassembly_text[binbytes_line_start:end_of_binbytes_section])
            # Update last_end
            last_end = end_of_binbytes_section

    # Append any remaining text after the last match
    output_parts.append(disassembly_text[last_end:])

    return "".join(output_parts)


def get_meta_and_pickle(uri: str) -> tuple[ty.Optional[metadata.ResultMetadata], str]:
    """To be used when the issue is internal to the pickle itself."""

    def _replace_all_dis_numbers(
        disassembly_text: str,
    ) -> str:
        # Replace all line numbers with a placeholder
        lines = disassembly_text.splitlines()
        return "\n".join([re.sub(r"^(\s*)\d+:", r"\1 ", line) for line in lines])

    if uri.endswith("/" + strings.INVOCATION):
        _, invoc_raw = read_partial_pickle(uris.get_bytes(uri, type_hint=strings.INVOCATION))
        # the raw invocation itself contains a nested pickle. we want to show the outer opcodes of the
        # raw invocation, and then we _also_ want to pull out the inner args_kwargs_pickle and show
        # the opcodes of that one, preferably without repeating ourselves too much.
        invoc_dis = _dis(invoc_raw)
        return None, _replace_all_dis_numbers(replace_all_nested_pickles(invoc_dis))

    # TODO maybe handle exception type hinting here?
    meta_bytes, first_pickle = read_partial_pickle(uris.get_bytes(uri, type_hint=results.RESULT))
    return (
        metadata.parse_result_metadata(meta_bytes.decode("utf-8").split("\n")),
        _replace_all_dis_numbers(_dis(first_pickle)),
    )
