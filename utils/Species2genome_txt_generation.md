# Database Generation Instruction

## metaphlan_map

### Method to generate `defined_species2genomes.txt`

1. Follow the instruction in `buildIDlists.pl` to prepare reference information files.
2. Run customized `buildIDlists.pl` with original `species2genome.txt` file once and obtain the error output.
3. Obtain the "Undefined" bioproject ID (PRJNAxxx) from error output. Note that the "customized" script is provided in our repo: <https://github.com/BGI-flexlab/SOAPMetaS/blob/dev_maple/utils/buildIDlists.pl>.
4. Grep lines in original "species2genome.txt" which don't contain "Undefined" bioproject ID, the output lines form `defined_species2genomes.txt`

