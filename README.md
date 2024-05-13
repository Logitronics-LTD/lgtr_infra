# Read package version from pyproject.toml
PKG_VER=$(python -c "import toml; print(toml.load('pyproject.toml')['tool']['poetry']['version'])")

# Create a git tag based on the package version
git tag -a "$PKG_VER" -m "Release version"