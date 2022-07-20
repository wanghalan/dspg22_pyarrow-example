# dspg22_pyarrow-example
Demonstrate > 50 MB limits storage on GitHub using PyArrow

Usage
---
```python
usage: divider.py [-h] -i INPUT [-s SIZE] -o OUTPUT [-v | --verbose | --no-verbose]

Take a file and divide it into partitions of specific sizes

options:
  -h, --help            show this help message and exit
  -i INPUT, --input INPUT
                        The large file to be partitioned
  -s SIZE, --size SIZE  Maximum size of the partitioned file in MB
  -o OUTPUT, --output OUTPUT
  -v, --verbose, --no-verbose
```

Example
---
To generate the files in this repository, I did:
```python
python divider.py -i output_2019_q1.parquet -o ookla-dataset
```

References
---
- [What is Parquet](https://databricks.com/glossary/what-is-parquet)
- [PyArrow documentation](https://arrow.apache.org/docs/python/install.html)
- [Notes](https://www.overleaf.com/read/zqkmnghsffpc)
