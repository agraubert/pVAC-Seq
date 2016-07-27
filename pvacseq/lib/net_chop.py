import argparse
import sys
import requests
import csv
import tempfile
import re
import os
from time import sleep
from lib.main import split_file
import threading

cycle = ['|', '/', '-', '\\']
methods = ['cterm', '20s']
jobid_searcher = re.compile(r'<!-- jobid: [0-9a-fA-F]*? status: (queued|active)')
result_delimiter = re.compile(r'-{20,}')
fail_searcher = re.compile(r'(Failed run|Problematic input:)')

def main(args_input = sys.argv[1:]):
    parser = argparse.ArgumentParser("pvacseq net_chop")
    parser.add_argument(
        'input_file',
        type=argparse.FileType('r'),
        help="Input filtered file with predicted epitopes"
    )
    parser.add_argument(
        'output_file',
        type=argparse.FileType('w'),
        help="Output tsv filename for putative neoepitopes"
    )
    parser.add_argument(
        '--method',
        choices=methods,
        help="NetChop prediction method to use (\"cterm\" for C term 3.0, \"20s\" for 20S 3.0).  Default: \"cterm\" (C term 3.0)",
        default='cterm'
    )
    parser.add_argument(
        '--threshold',
        type=float,
        help="NetChop prediction threshold.  Default: 0.5",
        default=0.5
    )
    parser.add_argument(
        '--parallelize',
        type=int,
        help="Maximum number of connections to open in parallel to NetChop.  This has no effect on the order of the output; variants are always output in the same order as they appear in the input file.  Default: 1 (No parallelization)",
        default = 1
    )
    args = parser.parse_args(args_input)
    chosen_method = str(methods.index(args.method))
    reader = csv.DictReader(args.input_file, delimiter='\t')
    writer = csv.DictWriter(
        args.output_file,
        reader.fieldnames+['Best Cleavage Position', 'Best Cleavage Score'],
        delimiter='\t',
        lineterminator='\n'
    )
    writer.writeheader()
    x = 0
    i=1
    print("Waiting for results from NetChop... |", end='')
    threads = []
    last_thread = (-1, None)
    sys.stdout.flush()
    for chunk in split_file(reader, 100):
        staging_file = tempfile.NamedTemporaryFile(mode='w+')
        current_buffer = {}
        for line in chunk:
            sequence_id = '%010x'%x
            staging_file.write('>'+sequence_id+'\n')
            staging_file.write(line['MT Epitope Seq']+'\n')
            current_buffer[sequence_id] = {k:line[k] for k in line}
            x+=1
        staging_file.seek(0)
        if len(threads)<=args.parallelize:
            threads.append(threading.Thread(target=_netchop_thread, args=(
                staging_file,
                {k:current_buffer[k] for k in current_buffer},
                writer,
                chosen_method,
                '%0f'%args.threshold,
                last_thread[1]
            )))
            last_thread = (len(threads), threads[-1])
        else:
            k = 0
            while True:
                sys.stdout.write('\b'+cycle[i%4])
                sys.stdout.flush()
                threads[k].join(1)
                if last_thread[0] != k and not threads[k].is_alive():
                    threads[k] = threading.Thread(target=_netchop_thread, args=(
                        staging_file,
                        {k:current_buffer[k] for k in current_buffer},
                        writer,
                        chosen_method,
                        '%0f'%args.threshold,
                        last_thread[1]
                    ))
                    last_thread = (k, threads[k])
                    break
                k+=1
                i+=1
        last_thread[1].start()
        sleep(1)
        sys.stdout.write('\b'+cycle[i%4])
        sys.stdout.flush()
        i+=1
    while last_thread[1].is_alive():
        sys.stdout.write('\b'+cycle[i%4])
        sys.stdout.flush()
        last_thread[1].join(1)
        i+=1
    sys.stdout.write('\b\b')
    print("OK")
    args.output_file.close()
    args.input_file.close()

def _netchop_thread(staging_file, chunkbuffer, writer, method, threshold, previous_thread):
    jobid_searcher = re.compile(r'<!-- jobid: [0-9a-fA-F]*? status: (queued|active)')
    result_delimiter = re.compile(r'-+')
    fail_searcher = re.compile(r'(Failed run|Problematic input:)')
    response = requests.post(
        "http://www.cbs.dtu.dk/cgi-bin/webface2.fcgi",
        files={'SEQSUB':(staging_file.name, staging_file, 'text/plain')},
        data = {
            'configfile':'/usr/opt/www/pub/CBS/services/NetChop-3.1/NetChop.cf',
            'SEQPASTE':'',
            'method':method,
            'thresh':threshold
        }
    )
    q=0
    while jobid_searcher.search(response.content.decode()):
        sleep(10)
        q+=1
        response = requests.get(response.url)
    if fail_searcher.search(response.content.decode()):
        sys.stdout.write('\b\b')
        print('Failed!')
        print("NetChop encountered an error during processing")
        sys.exit(1)
    if previous_thread:
        previous_thread.join()
    results = [item.strip() for item in result_delimiter.split(response.content.decode())]
    for i in range(2, len(results), 4): #examine only the parts we want, skipping all else
        score = -1
        pos = 0
        sequence_name = False
        for line in results[i].split('\n'):
            data = [word for word in line.strip().split(' ') if len(word)]
            currentPosition = data[0]
            currentScore = float(data[3])
            if not sequence_name:
                sequence_name = data[4]
            if currentScore > score:
                score = currentScore
                pos = currentPosition
        line = current_buffer[sequence_name]
        line.update({
            'Best Cleavage Position':pos,
            'Best Cleavage Score':score
        })
        writer.writerow(line)


if __name__ == '__main__':
    main()
