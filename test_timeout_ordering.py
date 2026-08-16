"""Static regression checks for Discord interaction acknowledgement ordering."""
import ast
from pathlib import Path

SOURCE = Path(__file__).with_name("main.py")
TEXT = SOURCE.read_text(encoding="utf-8")
TREE = ast.parse(TEXT)


def dotted(node):
    if isinstance(node, ast.Call):
        return dotted(node.func)
    if isinstance(node, ast.Attribute):
        base = dotted(node.value)
        return f"{base}.{node.attr}" if base else node.attr
    if isinstance(node, ast.Name):
        return node.id
    return ""


def command_functions():
    for function in (node for node in TREE.body if isinstance(node, ast.AsyncFunctionDef)):
        if any(
            isinstance(decorator, ast.Call)
            and isinstance(decorator.func, ast.Attribute)
            and decorator.func.attr == "command"
            for decorator in function.decorator_list
        ):
            yield function


def first_ack_line(function):
    acknowledgements = []
    for node in ast.walk(function):
        if not isinstance(node, ast.Await) or not isinstance(node.value, ast.Call):
            continue
        name = dotted(node.value.func)
        if name in {
            "interaction.response.defer",
            "interaction.response.send_message",
            "interaction.response.edit_message",
            "interaction.response.send_modal",
        }:
            acknowledgements.append(node.lineno)
    return min(acknowledgements) if acknowledgements else None


def test_all_commands_have_no_io_before_ack():
    commands = list(command_functions())
    assert len(commands) == 63
    dangerous_fragments = (
        "db_", "conn.cursor", ".fetch_", ".create_invite", ".send", "ensure_broadcast_feature_tables",
    )
    for function in commands:
        ack_line = first_ack_line(function)
        assert ack_line is not None, function.name
        for node in ast.walk(function):
            if not isinstance(node, ast.Call) or node.lineno >= ack_line:
                continue
            name = dotted(node)
            assert not any(fragment in name for fragment in dangerous_fragments), (
                function.name, node.lineno, name
            )


def test_autocomplete_is_snapshot_only():
    names = {
        "broadcast_template_autocomplete",
        "broadcast_templates_name_autocomplete",
        "checkplayer_autocomplete",
        "cleanup_autocomplete",
    }
    found = set()
    for function in (node for node in ast.walk(TREE) if isinstance(node, ast.AsyncFunctionDef)):
        if function.name not in names:
            continue
        found.add(function.name)
        calls = {dotted(node) for node in ast.walk(function) if isinstance(node, ast.Call)}
        assert not any("db_" in call or "conn.cursor" in call for call in calls), function.name
    assert found == names


def test_all_ui_callbacks_ack_before_io():
    acknowledgement_names = {
        "interaction.response.defer",
        "interaction.response.send_message",
        "interaction.response.edit_message",
        "interaction.response.send_modal",
    }
    dangerous_fragments = (
        "db_", "conn.cursor", ".fetch_", ".create_invite", ".send",
        "ensure_broadcast_feature_tables", "load_snapshot", "increment_invite",
    )
    checked = []
    for class_node in (node for node in ast.walk(TREE) if isinstance(node, ast.ClassDef)):
        methods = {
            node.name: node for node in class_node.body if isinstance(node, ast.AsyncFunctionDef)
        }
        for function in methods.values():
            is_button = any(
                isinstance(decorator, ast.Call) and dotted(decorator.func).endswith("discord.ui.button")
                for decorator in function.decorator_list
            )
            if function.name not in {"callback", "on_submit"} and not is_button:
                continue
            checked.append(f"{class_node.name}.{function.name}")
            ack_lines = [
                node.lineno for node in ast.walk(function)
                if isinstance(node, ast.Await)
                and isinstance(node.value, ast.Call)
                and dotted(node.value.func) in acknowledgement_names
            ]
            ack_line = min(ack_lines) if ack_lines else None
            # Tiny page-button wrappers delegate immediately to a class helper;
            # inspect that helper's acknowledgement as part of this callback.
            if ack_line is None and function.body and isinstance(function.body[0], ast.Expr):
                first_value = function.body[0].value
                if isinstance(first_value, ast.Await) and isinstance(first_value.value, ast.Call):
                    helper_name = dotted(first_value.value.func).removeprefix("self.")
                    helper = methods.get(helper_name)
                    helper_acks = [
                        node.lineno for node in ast.walk(helper)
                        if isinstance(node, ast.Await)
                        and isinstance(node.value, ast.Call)
                        and dotted(node.value.func) in acknowledgement_names
                    ] if helper else []
                    assert helper_acks, f"{class_node.name}.{function.name}"
            else:
                assert ack_line is not None, f"{class_node.name}.{function.name}"
                for node in ast.walk(function):
                    if not isinstance(node, ast.Call) or node.lineno >= ack_line:
                        continue
                    name = dotted(node)
                    assert not any(fragment in name for fragment in dangerous_fragments), (
                        class_node.name, function.name, node.lineno, name
                    )
    assert len(checked) == 41


if __name__ == "__main__":
    test_all_commands_have_no_io_before_ack()
    test_autocomplete_is_snapshot_only()
    test_all_ui_callbacks_ack_before_io()
    print("MCWV BOT timeout ordering checks passed")
