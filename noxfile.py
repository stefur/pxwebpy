from nox import Session, options
from nox_uv import session

options.default_venv_backend = "uv"


@session(
    name="tests",
    python=["3.10", "3.11", "3.12", "3.13", "3.14"],
    uv_groups=["dev"],
)
def test(s: Session) -> None:
    s.run("python", "-m", "pytest", "-vv")
