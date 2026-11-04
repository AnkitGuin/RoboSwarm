"""Generate a unique local PostgreSQL secret without modifying tracked files."""
from pathlib import Path
import secrets


def main() -> None:
    target = Path(".env")
    if target.exists():
        print("Existing .env preserved; verify POSTGRES_PASSWORD is set.")
        return
    template = Path(".env.example").read_text()
    if "POSTGRES_PASSWORD=\n" not in template:
        raise RuntimeError("Missing POSTGRES_PASSWORD placeholder in .env.example")
    target.write_text(template.replace("POSTGRES_PASSWORD=\n",
                                       "POSTGRES_PASSWORD=" + secrets.token_urlsafe(24) + "\n"))
    target.chmod(0o600)
    print("Created .env with a random local-development PostgreSQL password")


if __name__ == "__main__":
    main()
