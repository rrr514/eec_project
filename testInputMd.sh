make scheduler
make simulator
./simulator inputs/Day.md > "log.txt"
# Check if the output file is created
if [ -f "log.txt" ]; then
	echo "Output file created successfully."
else
	echo "Failed to create output file."
fi