from importlib_metadata import version as get_version

version = get_version("pxwebpy")

version_var = f"""VERSION={version}"""

with open("_environment", "w") as env:
    env.write(version_var)
