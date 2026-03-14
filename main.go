package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"io/ioutil"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// Config structure for configuration file
type Config struct {
	DB        string `json:"db"`
	ThumbDir  string `json:"thumb_dir"`
	DryRun    bool   `json:"dry_run"`
}

// LoadConfig reads the configuration from a JSON file.
func LoadConfig(configPath string) (*Config, error) {
	// Read the config file
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("could not read config file: %w", err)
	}

	// Parse the JSON data
	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("could not parse config file: %w", err)
	}

	return &config, nil
}

// custom help message
func printHelp() {
	fmt.Println("Usage: go run main.go [options]")
	fmt.Println("\nOptions:")
	fmt.Println("  --db          Path to the SQLite database (default: ./files.db).")
	fmt.Println("  --thumb-dir   Path to the thumbnail directory (default: ./thumbnails).")
	fmt.Println("  --dry-run     Simulate the deletion of files without actually deleting them (default: true).")
	fmt.Println("  -h, --help    Show this help message.")
	fmt.Println("\nExample usage:")
	fmt.Println("  go run main.go --db ./mydb.db --thumb-dir ./images --dry-run")
	fmt.Println("  go run main.go --db ./mydb.db --thumb-dir ./images")
}

func main() {
	// Command-line flags
	configPath := flag.String("config", "./config.json", "Path to the JSON configuration file.")
	dbPath := flag.String("db", "", "Path to the SQLite database (override config).")
	thumbnailDir := flag.String("thumb-dir", "", "Path to the thumbnail directory (override config).")
	dryRun := flag.Bool("dry-run", true, "Simulate the deletion of files without actually deleting them (override config).")
	help := flag.Bool("h", false, "Show help message.") // Added flag for help message
	flag.Parse()

	// If help flag is set, print help message and exit
	if *help {
		printHelp()
		return
	}

	// Load the config from the config file
	var config *Config
	var err error
	if _, err := os.Stat(*configPath); err == nil {
		// If the config file exists, load it
		config, err = LoadConfig(*configPath)
		if err != nil {
			fmt.Printf("Error loading config file: %v\n", err)
			return
		}
	} else {
		fmt.Println("Config file not found, using default values.")
	}

	// Override the values from command-line flags if provided
	if *dbPath != "" {
		config.DB = *dbPath
	}
	if *thumbnailDir != "" {
		config.ThumbDir = *thumbnailDir
	}
	if !*dryRun {
		config.DryRun = false
	}

	// Print out the config for verification
	fmt.Printf("Using config: DB=%s, ThumbDir=%s, DryRun=%t\n", config.DB, config.ThumbDir, config.DryRun)

	// Open SQLite database
	db, err := sql.Open("sqlite3", config.DB)
	if err != nil {
		fmt.Printf("Failed to connect to SQLite database: %v\n", err)
		return
	}
	defer db.Close()

	// Regex for the updated file name pattern {hash}_{size}_{dimension}.jpg
	filePattern := regexp.MustCompile(`^([a-fA-F0-9]+)_(\d+x\d+)_(\w+)\.jpg$`)

	// List of files to delete
	var filesToDelete []string

	// Prepare the SQL statement for checking hash existence (now using file_hash)
	stmt, err := db.Prepare(`SELECT COUNT(*) FROM Files WHERE file_hash = ?`)
	if err != nil {
		fmt.Printf("Failed to prepare SQL statement: %v\n", err)
		return
	}
	defer stmt.Close()

	// Traverse the directory
	err = filepath.WalkDir(config.ThumbDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("error accessing path %s: %w", path, err)
		}

		// Skip directories
		if d.IsDir() {
			return nil
		}

		// Match file name
		fileName := d.Name()
		matches := filePattern.FindStringSubmatch(fileName)
		if len(matches) != 4 {
			fmt.Printf("Skipping invalid file name: %s\n", fileName)
			return nil
		}

		// Extract hash from file name
		hash := matches[1]

		// Check if hash exists in the Files table (using file_hash)
		exists, err := checkHashExists(stmt, hash)
		if err != nil {
			return fmt.Errorf("failed to check file_hash %s: %w", hash, err)
		}

		if !exists {
			// Mark for deletion
			fmt.Printf("Marking for deletion: %s\n", path)
			filesToDelete = append(filesToDelete, path)
		}

		return nil
	})

	if err != nil {
		fmt.Printf("Error during directory traversal: %v\n", err)
		return
	}

	// Perform actual deletion or dry-run simulation
	for _, file := range filesToDelete {
		if config.DryRun {
			fmt.Printf("[Dry Run] File would be deleted: %s\n", file)
		} else {
			fmt.Printf("Deleting file: %s\n", file)
			if err := os.Remove(file); err != nil {
				fmt.Printf("Failed to delete file %s: %v\n", file, err)
			}
		}
	}

	// Print the number of affected files using len(filesToDelete)
	if len(filesToDelete) > 0 {
		fmt.Printf("\nTotal number of affected files: %d\n", len(filesToDelete))
	} else {
		fmt.Println("\nNo files were marked for deletion.")
	}

	if config.DryRun {
		fmt.Println("Dry run completed. No files were deleted.")
	} else {
		fmt.Println("Cleanup completed.")
	}
}

// checkHashExists checks if a given hash exists in the Files table using a prepared statement.
func checkHashExists(stmt *sql.Stmt, hash string) (bool, error) {
	var count int
	err := stmt.QueryRow(hash).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

