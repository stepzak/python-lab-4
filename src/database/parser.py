import datetime
import shlex
import dataclasses

from src.database.query_plan import QueryPlan, QueryType


class QueryParser:
    @staticmethod
    def parse(query: str) -> QueryPlan | None:
        raise NotImplementedError
        shlex_splitted = shlex.split(query)
        if not shlex_splitted:
            return None
        shlex_splitted[0] = shlex_splitted[0].lower()
        match shlex_splitted[0]:
            case "create":
                QueryParser._process_create(shlex_splitted[1:])
            case "select":
                QueryParser._process_select(shlex_splitted[1:])


    @staticmethod
    def _process_create(splitted: list[str],):
        typeof = splitted[0].lower()
        match typeof:
            case "dtype":
                return QueryParser._process_create_dtype(splitted[1:])
            case "table":
                return QueryParser._process_create_table(splitted[1:])
            case _:
                raise SyntaxError(f"Unknown query type: CREATE {typeof}")


    @staticmethod
    def _process_create_dtype(splitted: list[str]):
        types = {
            "int": int,
            "float": float,
            "str": str,
            "bool": bool,
            "datetime": datetime,
        }
        fields = []
        name = splitted[0]
        if name.endswith("("):
            name = name[:-1]
        else:
            if not splitted[1].startswith("("):
                raise SyntaxError(f"Expected '(' near {name}")
        state = 0
        field = []
        for token in splitted[1:]:
            if token in (",", ")"):
                continue
            if token == ")":
                if state == 1:
                    token = token[:-1]
                    field.append(types[token])
                fields.append(tuple(field))
                break
            if state == 1:
                token = types[token]
            field.append(token)
            state = ~state

        dtype = dataclasses.make_dataclass(name, fields)
        return QueryPlan(
            table = '',
            kwargs = {"dtype": dtype},
            operation = QueryType.CREATE
        )

    @staticmethod
    def _process_create_table(splitted: list[str],):
        name = splitted[0]
        if name.endswith("("):
            name = name[:-1]
            dtype = splitted[1]
        else:
            if not splitted[1].startswith("("):
                raise SyntaxError(f"Expected '(' near {name}")
            dtype = splitted[2]

        if dtype.endswith(")"):
            dtype = dtype[:-1]

        return QueryPlan(
            operation = QueryType.CREATE,
            table = name,
            kwargs = {"dtype": dtype},
        )

    @staticmethod
    def _process_select(splitted: list[str],):
        op = splitted[0].lower()
        if op.lower() != "from":
            raise SyntaxError(f"Expected 'from' (got {op})")

        name = splitted[1]
        ind = 2
        for token in splitted[2:]:
            if token.lower() == "where":
                break
            ind += 1
        filters = QueryParser._process_filters(splitted[ind+1:])
        return QueryPlan(
            operation = QueryType.SELECT,
            table = name,
            kwargs = {"filters": filters},
        )

    @staticmethod
    def _process_insert(splitted: list[str],):
        op = splitted[0].lower()
        if op != "into":
            raise SyntaxError(f"Expected 'into' (got {op})")
        name = splitted[1]


    @staticmethod
    def _process_filters(splitted: list[str],):
        op_map = {
            "=": "eq",
            ">": "gt",
            "<": "lt",
            ">=": "ge",
            "<=": "le",
            "in": "in"
        }
        state = 0
        field = op = ""
        filters = {}
        for token in splitted:
            match state:
                case 0:
                    field = token
                case 1:
                    op = op_map[token]
                case 2:
                    val = token
                    filters[f"{field}__{op}"] = val
                    field = op = ""

            state = (state + 1) % 3

        return filters


