all:
	pylint *.py

clean:
	rm -rf *.pyc __pycache__
