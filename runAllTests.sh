#!/bin/bash
# filepath: /u/rrr/eec_project/run_all.sh

# Ensure the outputs directory exists
mkdir -p outputs

# Compile scheduler and simulator
make scheduler
make simulator

# Loop over each Markdown file in the inputs directory
for input in inputs/*.md; do
    filename=$(basename "$input" .md)
    output="outputs/${filename}.txt"
    echo "Running simulation for $filename..."
    ./simulator "$input" > "$output"
done

echo "All simulations completed."