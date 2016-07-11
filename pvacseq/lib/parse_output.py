import argparse
import csv
import re
import operator
import sys
import os
from math import ceil
from statistics import median

def prediction_method_lookup(prediction_method):
    prediction_method_lookup_dict = {
        'netmhcpan' : 'NetMHCpan',
        'ann'       : 'NetMHC',
        'smmpmbec'  : 'SMMPMBEC',
        'smm'       : 'SMM',
        'netmhccons': 'NetMHCcons',
        'pickpocket': 'PickPocket',
    }
    return prediction_method_lookup_dict[prediction_method]

def protein_identifier_for_label(key_file):
    tsv_reader = csv.reader(key_file, delimiter='\t')
    pattern = re.compile('>')
    key_hash = {}
    for line in tsv_reader:
        new_name      = line[0]
        original_name = line[1]
        original_name = pattern.sub('', original_name)
        key_hash[new_name] = original_name

    return key_hash

def min_match_count(peptide_length):
    return ceil(peptide_length / 2)

def determine_consecutive_matches(mt_epitope_seq, wt_epitope_seq):
    consecutive_matches = 0
    left_padding        = 0
    #Count consecutive matches from the beginning of the epitope sequences
    for a, b in zip(mt_epitope_seq, wt_epitope_seq):
        if a == b:
            consecutive_matches += 1
            left_padding        += 1
        else:
            break
    #Count consecutive matches from the end of the epitope sequences
    for a, b in zip(reversed(mt_epitope_seq), reversed(wt_epitope_seq)):
        if a == b:
            consecutive_matches += 1
        else:
            break
    return consecutive_matches, left_padding

def parse_input_tsv_file(input_tsv_file):
    tsv_reader = csv.DictReader(input_tsv_file, delimiter='\t')
    tsv_entries = {}
    for line in tsv_reader:
        tsv_entries[line['index']] = line
    return tsv_entries

def match_wildtype_and_mutant_entries(iedb_results, wt_iedb_results):
    for key, result in iedb_results.items():
        (wt_iedb_result_key, mt_position) = key.split('|', 1)
        if result['variant_type'] == 'missense':
            iedb_results[key]['wt_epitope_seq'] = wt_iedb_results[wt_iedb_result_key][mt_position]['wt_epitope_seq']
            iedb_results[key]['wt_scores']      = wt_iedb_results[wt_iedb_result_key][mt_position]['wt_scores']
        else:
            wt_results        = wt_iedb_results[wt_iedb_result_key]
            mt_epitope_seq    = result['mt_epitope_seq']
            best_match_count  = 0
            best_left_padding = 0
            for wt_position, wt_result in wt_results.items():
                wt_epitope_seq = wt_result['wt_epitope_seq']

                consecutive_matches, left_padding = determine_consecutive_matches(mt_epitope_seq, wt_epitope_seq)
                if consecutive_matches > best_match_count:
                    best_match_count    = consecutive_matches
                    best_left_padding   = left_padding
                    best_match_position = wt_position
                elif consecutive_matches == best_match_count and left_padding > best_left_padding:
                    best_left_padding   = left_padding
                    best_match_position = wt_position

            if best_match_count >= min_match_count(int(iedb_results[key]['peptide_length'])):
                iedb_results[key]['wt_epitope_seq'] = wt_iedb_results[wt_iedb_result_key][best_match_position]['wt_epitope_seq']
                iedb_results[key]['wt_scores']      = wt_iedb_results[wt_iedb_result_key][best_match_position]['wt_scores']
            else:
                iedb_results[key]['wt_epitope_seq'] = 'NA'
                iedb_results[key]['wt_scores']      =  dict.fromkeys(iedb_results[key]['mt_scores'].keys(), 'NA')

    return iedb_results

def parse_iedb_file(input_iedb_files, tsv_entries, key_file):
    protein_identifier_from_label = protein_identifier_for_label(key_file)
    iedb_results = {}
    wt_iedb_results = {}
    for input_iedb_file in input_iedb_files:
        iedb_tsv_reader = csv.DictReader(input_iedb_file, delimiter='\t')
        (sample, allele_tmp, peptide_length_tmp, method, file_extension) = input_iedb_file.name.split(".", 4)
        for line in iedb_tsv_reader:
            protein_label  = line['seq_num']
            position       = line['start']
            epitope        = line['peptide']
            score          = line['ic50']
            allele         = line['allele']
            peptide_length = line['length']

            if protein_identifier_from_label[protein_label] is not None:
                protein_identifier = protein_identifier_from_label[protein_label]

            (protein_type, tsv_index) = protein_identifier.split('.', 1)
            if protein_type == 'MT':
                tsv_entry = tsv_entries[tsv_index]
                key = "%s|%s" % (tsv_index, position)
                if key not in iedb_results:
                    iedb_results[key] = {}
                    iedb_results[key]['mt_scores']         = {}
                    iedb_results[key]['mt_epitope_seq']    = epitope
                    iedb_results[key]['gene_name']         = tsv_entry['gene_name']
                    iedb_results[key]['amino_acid_change'] = tsv_entry['amino_acid_change']
                    iedb_results[key]['variant_type']      = tsv_entry['variant_type']
                    iedb_results[key]['position']          = position
                    iedb_results[key]['tsv_index']         = tsv_index
                    iedb_results[key]['allele']            = allele
                    iedb_results[key]['peptide_length']    = peptide_length
                iedb_results[key]['mt_scores'][method] = float(score)

            if protein_type == 'WT':
                if tsv_index not in wt_iedb_results:
                    wt_iedb_results[tsv_index] = {}
                if position not in wt_iedb_results[tsv_index]:
                    wt_iedb_results[tsv_index][position] = {}
                    wt_iedb_results[tsv_index][position]['wt_scores']         = {}
                wt_iedb_results[tsv_index][position]['wt_epitope_seq']    = epitope
                wt_iedb_results[tsv_index][position]['wt_scores'][method] = float(score)

    return match_wildtype_and_mutant_entries(iedb_results, wt_iedb_results)

def flatten_iedb_results(iedb_results):
    #transform the iedb_results dictionary into a two-dimensional list
    flattened_iedb_results = []
    for value in iedb_results.values():
        result_line = [
            value['gene_name'],
            value['amino_acid_change'],
            value['position'],
            value['mt_scores'],
            value['wt_scores'],
            value['wt_epitope_seq'],
            value['mt_epitope_seq'],
            value['tsv_index'],
            value['allele'],
            value['peptide_length'],
        ]
        mt_scores     = value['mt_scores']
        best_mt_score = sys.maxsize
        for method, score in mt_scores.items():
            if score < best_mt_score:
                best_mt_score        = score
                best_mt_score_method = method
        result_line.append(best_mt_score)
        result_line.append(value['wt_scores'][best_mt_score_method])
        result_line.append(best_mt_score_method)
        result_line.append(median(mt_scores.values()))
        flattened_iedb_results.append(result_line)

    return flattened_iedb_results

def sort_iedb_results(flattened_iedb_results):
    sorted_iedb_results = sorted(
        flattened_iedb_results,
        key=lambda flattened_iedb_results: (flattened_iedb_results[0], flattened_iedb_results[1], flattened_iedb_results[10], " ".join(str(item) for item in flattened_iedb_results))
    )

    return sorted_iedb_results

def process_input_iedb_file(input_iedb_files, tsv_entries, key_file):
    iedb_results           = parse_iedb_file(input_iedb_files, tsv_entries, key_file)
    flattened_iedb_results = flatten_iedb_results(iedb_results)
    sorted_iedb_results    = sort_iedb_results(flattened_iedb_results)

    return sorted_iedb_results

def base_headers():
    return[
        'Chromosome',
        'Start',
        'Stop',
        'Reference',
        'Variant',
        'Transcript',
        'Ensembl Gene ID',
        'Variant Type',
        'Mutation',
        'Protein Position',
        'Gene Name',
        'HLA Allele',
        'Peptide Length',
        'Sub-peptide Position',
        'MT Epitope Seq',
        'WT Epitope Seq',
        'Best MT Score',
        'Corresponding WT Score',
        'Fold Change',
        'Best MT Score Method',
        'Median MT Score All Methods'
    ]

def output_headers(methods):
    headers = base_headers()
    for method in methods:
        pretty_method = prediction_method_lookup(method)
        headers.append("%s WT Score" % pretty_method)
        headers.append("%s MT Score" % pretty_method)

    return headers

def determine_prediction_methods(input_iedb_files):
    methods = []
    for input_iedb_file in input_iedb_files:
        (sample, allele_tmp, peptide_length_tmp, method, file_extension) = input_iedb_file.name.split(".", 4)
        methods.append(method)

    return methods

def main(args_input = sys.argv[1:]):
    parser = argparse.ArgumentParser('pvacseq parse_output')
    parser.add_argument('input_iedb_files', type=argparse.FileType('r'), nargs='+', help='Raw output file from Netmhc',)
    parser.add_argument('input_tsv_file', type=argparse.FileType('r'), help='Input list of variants')
    parser.add_argument('key_file', type=argparse.FileType('r'), help='Key file for lookup of FASTA IDs')
    parser.add_argument('output_file', type=argparse.FileType('w'), help='Parsed output file')
    args = parser.parse_args(args_input)

    methods = determine_prediction_methods(args.input_iedb_files)
    tsv_writer = csv.DictWriter(args.output_file, delimiter='\t', fieldnames=output_headers(methods))
    tsv_writer.writeheader()

    tsv_entries  = parse_input_tsv_file(args.input_tsv_file)
    iedb_results = process_input_iedb_file(args.input_iedb_files, tsv_entries, args.key_file)
    for gene_name, variant_aa, position, mt_scores, wt_scores, wt_epitope_seq, mt_epitope_seq, tsv_index, allele, peptide_length, best_mt_score, corresponding_wt_score, best_mt_score_method, median_mt_score in iedb_results:
        tsv_entry = tsv_entries[tsv_index]
        if mt_epitope_seq != wt_epitope_seq:
            if wt_epitope_seq == 'NA':
                fold_change = 'NA'
            else:
                fold_change = "%.3f" % (corresponding_wt_score/best_mt_score)
            row = {
                'Chromosome'          : tsv_entry['chromosome_name'],
                'Start'               : tsv_entry['start'],
                'Stop'                : tsv_entry['stop'],
                'Reference'           : tsv_entry['reference'],
                'Variant'             : tsv_entry['variant'],
                'Transcript'          : tsv_entry['transcript_name'],
                'Ensembl Gene ID'     : tsv_entry['ensembl_gene_id'],
                'Variant Type'        : tsv_entry['variant_type'],
                'Mutation'            : variant_aa,
                'Protein Position'    : tsv_entry['protein_position'],
                'Gene Name'           : gene_name,
                'HLA Allele'          : allele,
                'Peptide Length'      : peptide_length,
                'Sub-peptide Position': position,
                'MT Epitope Seq'      : mt_epitope_seq,
                'WT Epitope Seq'      : wt_epitope_seq,
                'Best MT Score'       : best_mt_score,
                'Corresponding WT Score': corresponding_wt_score,
                'Best MT Score Method': prediction_method_lookup(best_mt_score_method),
                'Median MT Score All Methods': median_mt_score,
                'Fold Change'         : fold_change,
            }
            for method in methods:
                pretty_method = prediction_method_lookup(method)
                row["%s WT Score" % pretty_method] = wt_scores[method]
                row["%s MT Score" % pretty_method] = mt_scores[method]
            tsv_writer.writerow(row)

if __name__ == '__main__':
    main()
