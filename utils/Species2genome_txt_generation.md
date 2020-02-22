# Database Generation Instruction

## metaphlan_map

### Method to generate `defined_species2genomes.txt`

1. Follow the instruction in `buildIDlists.pl` to prepare reference information files.
2. Run customized `buildIDlists.pl` \(modified the output error information\) with original `species2genome.txt` file once and obtain the error output.
3. Obtain the "Undefined" bioproject ID (PRJNAxxx) from error output.
4. Grep lines in original "species2genome.txt" which don't contain "Undefined" bioproject ID, the output lines form `defined_species2genomes.txt`

