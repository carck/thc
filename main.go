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
	"strings"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// Config structure for configuration file
type Config struct {
	DB        string `json:"db"`
	ThumbDir  string `json:"thumb_dir"`
	OriginDir string `json:"origin_dir"`
	DryRun    bool   `json:"dry_run"`
}

// LoadConfig reads the configuration from a JSON file.
func LoadConfig(configPath string) (*Config, error) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("could not read config file: %w", err)
	}

	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("could not parse config file: %w", err)
	}

	return &config, nil
}

func printHelp() {
	fmt.Println("Usage: go run main.go [options]")
	fmt.Println("\nOptions:")
	fmt.Println("  --config      Path to the JSON configuration file (default: ./config.json).")
	fmt.Println("  --db          Path to the SQLite database (override config).")
	fmt.Println("  --thumb-dir   Path to the thumbnail directory (override config).")
	fmt.Println("  --origin-dir  Path to the origin directory (override config).")
	fmt.Println("  --dry-run     Simulate the deletion of files without actually deleting them (override config).")
	fmt.Println("  -h, --help    Show this help message.")
	fmt.Println("\nExample usage:")
	fmt.Println("  go run main.go --db ./mydb.db --thumb-dir ./thumbs --origin-dir ./originals --dry-run")
}

func checkHashExists(stmt *sql.Stmt, hash string) (bool, error) {
	var count int
	err := stmt.QueryRow(hash).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func checkPathExists(stmt *sql.Stmt, path string) (bool, error) {
	var count int
	normalizedPath := strings.ReplaceAll(path, string(os.PathSeparator), "/")
	err := stmt.QueryRow(normalizedPath).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func cleanupThumbnails(db *sql.DB, dir string, dryRun bool) ([]string, error) {
	var filesToDelete []string
	
	stmt, err := db.Prepare(`SELECT COUNT(*) FROM Files WHERE file_hash = ?`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare SQL statement: %w", err)
	}
	defer stmt.Close()

	filePattern := regexp.MustCompile(`^([a-fA-F0-9]+)_(\d+x\d+)_(\w+)\.jpg$`)

	err = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("error accessing path %s: %w", path, err)
		}

		if d.IsDir() {
			return nil
		}

		fileName := d.Name()
		matches := filePattern.FindStringSubmatch(fileName)
		if len(matches) != 4 {
			fmt.Printf("Skipping invalid thumbnail: %s\n", path)
			return nil
		}

		hash := matches[1]
		exists, err := checkHashExists(stmt, hash)
		if err != nil {
			return fmt.Errorf("failed to check hash %s: %w", hash, err)
		}

		if !exists {
			filesToDelete = append(filesToDelete, path)
			fmt.Printf("Marking thumbnail for deletion: %s\n", path)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error during thumbnail directory traversal: %w", err)
	}

	return filesToDelete, nil
}

func cleanupOriginals(db *sql.DB, dir string, dryRun bool) ([]string, error) {
	var filesToDelete []string

	// 使用CONCAT(file_root, file_path)确保路径拼接正确
	stmt, err := db.Prepare(`SELECT COUNT(*) FROM Files WHERE file_root || file_path = ?`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare SQL statement: %w", err)
	}
	defer stmt.Close()

	err = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("error accessing path %s: %w", path, err)
		}

		if d.IsDir() {
			return nil
		}

		// 获取相对于origin目录的路径
		relPath, err := filepath.Rel(dir, path)
		if err != nil {
			return fmt.Errorf("failed to get relative path for %s: %w", path, err)
		}

		// 规范化路径分隔符为/
		normalizedPath := strings.ReplaceAll(relPath, string(os.PathSeparator), "/")
		
		// 检查路径是否存在于数据库中
		exists, err := checkPathExists(stmt, normalizedPath)
		if err != nil {
			return fmt.Errorf("failed to check path %s: %w", normalizedPath, err)
		}

		if !exists {
			filesToDelete = append(filesToDelete, path)
			fmt.Printf("Marking original for deletion: %s (db path: %s)\n", path, normalizedPath)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error during originals directory traversal: %w", err)
	}

	return filesToDelete, nil
}

func deleteFiles(files []string, dryRun bool) error {
	for _, file := range files {
		if dryRun {
			fmt.Printf("[Dry Run] File would be deleted: %s\n", file)
		} else {
			fmt.Printf("Deleting file: %s\n", file)
			if err := os.Remove(file); err != nil {
				return fmt.Errorf("failed to delete file %s: %w", file, err)
			}
		}
	}
	return nil
}

func main() {
	configPath := flag.String("config", "./config.json", "Path to the JSON configuration file.")
	dbPath := flag.String("db", "", "Path to the SQLite database (override config).")
	thumbDir := flag.String("thumb-dir", "", "Path to the thumbnail directory (override config).")
	originDir := flag.String("origin-dir", "", "Path to the origin directory (override config).")
	dryRun := flag.Bool("dry-run", true, "Simulate the deletion of files without actually deleting them (override config).")
	help := flag.Bool("h", false, "Show help message.")
	flag.Parse()

	if *help {
		printHelp()
		return
	}

	config, err := LoadConfig(*configPath)
	if err != nil {
		if !os.IsNotExist(err) {
			fmt.Printf("Error loading config file: %v\n", err)
			return
		}
		fmt.Println("Config file not found, using default values.")
		config = &Config{}
	}

	if *dbPath != "" {
		config.DB = *dbPath
	}
	if *thumbDir != "" {
		config.ThumbDir = *thumbDir
	}
	if *originDir != "" {
		config.OriginDir = *originDir
	}
	if !*dryRun {
		config.DryRun = false
	}

	fmt.Printf("Using config: DB=%s, ThumbDir=%s, OriginDir=%s, DryRun=%t\n", 
		config.DB, config.ThumbDir, config.OriginDir, config.DryRun)

	db, err := sql.Open("sqlite3", config.DB)
	if err != nil {
		fmt.Printf("Failed to connect to SQLite database: %v\n", err)
		return
	}
	defer db.Close()

	var (
		thumbFiles  []string
		originFiles []string
		totalFiles  int
	)

	if config.ThumbDir != "" {
		thumbFiles, err = cleanupThumbnails(db, config.ThumbDir, config.DryRun)
		if err != nil {
			fmt.Printf("Error cleaning thumbnails: %v\n", err)
			return
		}
		totalFiles += len(thumbFiles)
	}

	if config.OriginDir != "" {
		originFiles, err = cleanupOriginals(db, config.OriginDir, config.DryRun)
		if err != nil {
			fmt.Printf("Error cleaning originals: %v\n", err)
			return
		}
		totalFiles += len(originFiles)
	}

	// 统一处理删除操作
	if err := deleteFiles(thumbFiles, config.DryRun); err != nil {
		fmt.Printf("Error deleting thumbnails: %v\n", err)
	}
	if err := deleteFiles(originFiles, config.DryRun); err != nil {
		fmt.Printf("Error deleting originals: %v\n", err)
	}

	// 打印统计信息
	if totalFiles > 0 {
		fmt.Printf("\nTotal number of affected files: %d (thumbnails: %d, originals: %d)\n", 
			totalFiles, len(thumbFiles), len(originFiles))
	} else {
		fmt.Println("\nNo files were marked for deletion.")
	}

	if config.DryRun {
		fmt.Println("Dry run completed. No files were deleted.")
	} else {
		fmt.Println("Cleanup completed.")
	}
}
