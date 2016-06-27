import unittest
import os
import tempfile
from filecmp import cmp
from subprocess import call
import sys
import py_compile
import random
import csv

#python -m unittest tests/test_binding_filter.py
class BindingFilterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pVac_directory = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
        cls.binding_filter_path = os.path.join(cls.pVac_directory, "pvacseq", "lib", "binding_filter.py")
        cls.test_data_path= os.path.join(cls.pVac_directory, "tests", "test_data", "binding_filter")

    def test_binding_filter_runs_and_produces_expected_output(self):
        compiled_script_path = py_compile.compile(self.binding_filter_path)
        self.assertTrue(compiled_script_path)
        output_file = tempfile.NamedTemporaryFile()
        binding_filter_cmd = "%s  %s  %s %s" % (
            sys.executable,
            compiled_script_path,
            os.path.join(
                self.test_data_path,
                'Test.HLA-A29:02.9.netmhc.parsed.tsv'
            ),
            output_file.name
        )
        self.assertFalse(call([binding_filter_cmd], shell=True))
        self.assertTrue(cmp(
            output_file.name,
            os.path.join(self.test_data_path, "Test_filtered.tsv"),
            False
        ))

    def test_binding_filter_under_random_constraints(self):
        random.seed()
        for trial in range(5):
            binding_threshold = random.randint(1,499)
            fold_change = random.randint(0,100)
            reader = open(os.path.join(self.test_data_path, 'Test_filtered.tsv'), mode='r')
            csv_reader = csv.DictReader(reader, delimiter='\t')
            temp_filtered = tempfile.NamedTemporaryFile()
            writer = open(temp_filtered.name, mode='w')
            csv_writer = csv.DictWriter(
                writer,
                csv_reader.fieldnames,
                delimiter='\t',
                lineterminator='\n'
            )
            csv_writer.writeheader()
            for line in csv_reader:
                if (int(line['MT score']) < binding_threshold and
                        (sys.maxsize if line['Fold Change'] == 'NA' else float(line['Fold Change'])) > fold_change):
                    csv_writer.writerow(line)
            reader.close()
            writer.close()

            temp_out = tempfile.NamedTemporaryFile()
            binding_filter_cmd = "%s  %s  %s  %s -b %d -c %d" % (
                sys.executable,
                self.binding_filter_path,
                os.path.join(
                    self.test_data_path,
                    'Test.HLA-A29:02.9.netmhc.parsed.tsv'
                ),
                temp_out.name,
                binding_threshold,
                fold_change
            )
            self.assertFalse(call([binding_filter_cmd], shell=True))
            self.assertTrue(cmp(
                temp_out.name,
                temp_filtered.name,
                False
            ))
