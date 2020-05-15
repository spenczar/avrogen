from __future__ import annotations

from typing import Dict, List, IO

import argparse
import io
import json
import sys

import jinja2
import avro.schema


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--generate", type=str, nargs="+")
    parser.add_argument("--include", type=str, nargs="+")
    parser.add_argument("--out", type=str)
    args = parser.parse_args()
    run(args.generate, args.include or [], args.out)


def run(generate_files: List[str], include_files: List[str], out_file: str) -> None:
    names: avro.schema.Names = avro.schema.Names()
    for filename in include_files:
        # First, for all the 'include' files, load them into the namespace.
        with open(filename, "rb") as fp:
            avro.schema.SchemaFromJSONData(json.load(fp), names)
    # Next, for all the generation targets, parse their schemas.
    schemas: List[avro.schema.Schema] = []
    for filename in generate_files:
        with open(filename, "r") as fp:
            schemas.append(avro.schema.SchemaFromJSONData(json.load(fp), names))

    # Finally, generate output
    with open(out_file, "w") as fp:
        write_dataclasses(schemas, fp)


def avrotype_to_pytype(type_schema: avro.schema.Schema) -> str:
    if isinstance(type_schema, avro.schema.PrimitiveSchema):
        return {
            "null": "None",
            "string": "str",
            "bytes": "bytes",
            "long": "int",
            "int": "int",
            "float": "float",
            "double": "double",
            "boolean": "bool"
        }[type_schema.type]
    if isinstance(type_schema, avro.schema.UnionSchema):
        if is_optional_type(type_schema):
            optional = avrotype_to_pytype(type_schema.schemas[1])
            return "Optional[" + optional + "]"
    if isinstance(type_schema, avro.schema.ArraySchema):
        return "List[" + avrotype_to_pytype(type_schema.items) + "]"
    if isinstance(type_schema, avro.schema.MapSchema):
        return "Dict[str, " + avrotype_to_pytype(type_schema.values) + "]"
    if isinstance(type_schema, avro.schema.RecordSchema):
        # TODO
        raise(Exception(f"Unable to handle type {type(type_schema)}"))
    else:
        raise(Exception(f"Unable to handle type {type(type_schema)}"))


def is_optional_type(schema: avro.schema.UnionSchema) -> bool:
    if len(schema.schemas) != 2:
        return False
    if not isinstance(schema.schemas[0], avro.schema.PrimitiveSchema):
        return False
    return schema.schemas[0].type == "null"


def write_dataclasses(record_schemas: List[avro.schema.Schema], fp: IO):
    tpl = jinja2.Template("""from __future__ import annotations

import dataclasses
from typing import Dict, Final, List, Optional

@dataclasses.dataclass
{%- for schema in schemas %}
class {{ capital_case(schema.name) }}:
{%- if schema.doc %}\"\"\"{{schema.doc}}
\"\"\"{% endif -%}
{% for field in schema.fields %}
    {{field.name}}:{{ avrotype_to_pytype(field.type) }}{% if "default" in field._props %} = {{ repr(field.default) }}{% endif %}

{%- endfor %}
    _avro_fullname: Final[str] = "{{schema.avro_name.fullname}}"
    _avro_namespace: Final[str] = "{{schema.avro_name.namespace}}"


{% endfor %}
""")

    rendered = tpl.render(
        schemas=record_schemas,
        avrotype_to_pytype=avrotype_to_pytype,
        repr=repr,
        capital_case=capital_case,
    )
    fp.write(rendered)


def capital_case(snakecased: str) -> str:
    return ''.join(word.title() for word in snakecased.split("_"))


if __name__ == "__main__":
    main()
