from dataclasses import make_dataclass

from src.database.session import DatabaseSession
import shlex

import builtins

def str_to_type(type_name: str):
    try:
        return getattr(builtins, type_name)
    except AttributeError:
        raise ValueError(f"Unknown type: {type_name}")

class RoflQLSession:
    def __init__(self):
        self.session = DatabaseSession()
        self.cmd_to_func = {
            "select": self.session.select_rows,
            "delete": self.session.delete,
            "update": self.session.update,
            "insert": self.session.insert,
        }

    def start_session(self):
        prompt = ""
        while prompt != "q":
            while not prompt.endswith(";"):
                prompt += " " + input()

            self.process_prompt(prompt)
            prompt = ""
            continue

    def process_dtype_create(self, splitted):
        fields: list[tuple[str, type]] = []
        name = splitted[2]
        if name.endswith("("):
            name.replace("(", "")
        else:
            if not name[3].startswith("("):
                return f"Unexpected token: {name[3]}"
            splitted.pop(3)
        state = 0
        cur_field = splitted[3]
        for arg in splitted[3:]:

            if arg.endswith(")"):
                if state != 0:
                    return f"Unexpected token near {arg}"
                fields.append((cur_field, str_to_type(arg.lower())))
                state = 2
            elif arg == ',':
                continue
            match state:
                case 0:
                    cur_field = arg
                    state = 1
                case 1:
                    fields.append((cur_field, str_to_type(arg.lower())))
                case 2:
                    if arg.lower() == "if":
                        state = 3
                        continue
                    else:
                        return f"Unexpected token {arg}"
                case 3:
                    if arg.lower() == "not":
                        state = 4
                        continue
                    else:
                        return f"Unexpected token {arg}"
                case 4:
                    if arg.lower() == "exists":
                        state = 5
                        continue
                    else:
                        return f"Unexpected token {arg}"
        dclass = make_dataclass(cls_name=name, fields=fields)
        self.session.create_dtype(name, dclass, state == 5)
        return "CREATE DTYPE"

    def process_table_create(self, splitted):
        name = splitted[2]
        if name.endswith("("):
            name.replace("(", "")
        else:
            if not name[3].startswith("("):
                return f"Unexpected token: {name[3]}"
            splitted.pop(3)


    def process_create(self, splitted: list[str]):
        typeof = splitted[1].lower()
        types = {
            "dtype": self.process_dtype_create,
        }



    def process_prompt(self, prompt):

        splitted = shlex.split(prompt)
        splitted[-1] = splitted[-1][:-1]
        cmd = splitted[0].lower()
        try:
            func = self.cmd_to_func[cmd]
        except KeyError:
            if cmd == "create":
                return self.process_create(splitted)


if __name__ == "__main__":
    session = RoflQLSession()
    session.start_session()
