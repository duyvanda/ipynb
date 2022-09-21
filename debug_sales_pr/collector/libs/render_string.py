from jinja2 import Template, meta, Environment


def get_require_params_template(str_template: str) -> set:
    env = Environment()
    parsed_content = env.parse(str_template)
    return meta.find_undeclared_variables(parsed_content)


def is_valid_params_template(str_template: str, dct_params: dict) -> bool:
    return len(get_require_params_template(str_template) - set(dct_params.keys())) == 0


def render_template(str_template: str, dct_params: dict) -> str:
    if is_valid_params_template(str_template, dct_params):
        return Template(str_template).render(**dct_params)
    raise KeyError(
        "miss key: {0}".format(
            get_require_params_template(str_template) - set(dct_params.keys())
        )
    )
