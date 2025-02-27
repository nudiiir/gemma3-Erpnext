from setuptools import setup, find_packages

with open("requirements.txt") as f:
	install_requires = f.read().strip().split("\n")

# get version from __version__ variable in doppio_bot/__init__.py
from doppio_bot import __version__ as version

setup(
	name="doppio_bot",
	version=version,
	description="ChatGPT in the Desk, powered by React & OpenAI API",
	author="Hussain Nagaria",
	author_email="hussain@frappe.io",
	packages=find_packages(),
	zip_safe=False,
	include_package_data=True,
	install_requires=install_requires
)
