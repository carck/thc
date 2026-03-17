package main

import (
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
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

var imageExt = map[string]bool{
	".jpg":  true,
	".jpeg": true,
	".jxl":  true,
	".png":  true,
	".gif":  true,
	".bmp":  true,
	".webp": true,
	".avif": true,
	".heic": true,
	".heif": true,
	".tif":  true,
	".tiff": true,
	".svg":  true,
	".ico":  true,
}

var videoExt = map[string]bool{
	".mp4":  true,
	".m4v":  true,
	".mov":  true,
	".avi":  true,
	".mkv":  true,
	".webm": true,
	".flv":  true,
	".wmv":  true,
	".mpg":  true,
	".mpeg": true,
	".3gp":  true,
	".3g2":  true,
	".ts":   true,
	".m2ts": true,
	".mts":  true,
	".vob":  true,
	".ogv":  true,
}

const hashSize = 16 * 1024

// Hash returns the SHA1 hash of a file as string.
func Hash(fileName string) (string, error) {
	bytes, err := readHashBytes(fileName)
	if err != nil {
		return "", err
	}
	hash := sha1.New()
	if _, hErr := hash.Write(bytes); hErr != nil {
		return "", hErr
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func readHashBytes(filePath string) ([]byte, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	} else if fi.Size() <= hashSize {
		return os.ReadFile(filePath)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	firstBytes := make([]byte, hashSize/2)
	if _, e := file.ReadAt(firstBytes, 0); e != nil {
		return nil, fmt.Errorf("couldn't read first few bytes: %+v", e)
	}

	middleBytes := make([]byte, hashSize/4)
	if _, e := file.ReadAt(middleBytes, fi.Size()/2); e != nil {
		return nil, fmt.Errorf("couldn't read middle bytes: %+v", e)
	}

	lastBytes := make([]byte, hashSize/4)
	if _, e := file.ReadAt(lastBytes, fi.Size()-hashSize/4); e != nil {
		return nil, fmt.Errorf("couldn't read end bytes: %+v", e)
	}

	bytes := append(append(firstBytes, middleBytes...), lastBytes...)
	return bytes, nil
}

func MediaType(filename string) string {
	ext := strings.ToLower(filepath.Ext(filename))
	if imageExt[ext] {
		return "image"
	}
	if videoExt[ext] {
		return "video"
	}
	return "other"
}

// LoadConfig reads the configuration from a JSON file.
func LoadConfig(configPath string) (*Config, error) {
	config := &Config{DryRun: true}

	data, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return config, nil
		}
		return nil, fmt.Errorf("could not read config file: %w", err)
	}

	if err := json.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("could not parse config file: %w", err)
	}

	return config, nil
}

func printHelp() {
	fmt.Println("Usage: go run main.go [options]")
	fmt.Println("\nOptions:")
	fmt.Println("  --config      Path to the JSON configuration file (default: ./config.json).")
	fmt.Println("  --db          Path to the SQLite database (override config).")
	fmt.Println("  --thumb-dir   Path to the thumbnail directory (override config).")
	fmt.Println("  --origin-dir  Path to the origin directory (override config).")
	fmt.Println("  --dry-run     Simulate the deletion of files without actually deleting them (default: true).")
	fmt.Println("  -h, --help    Show this help message.")
	fmt.Println("\nExample usage:")
	fmt.Println("  go run main.go --db ./mydb.db --thumb-dir ./thumbs --origin-dir ./originals --dry-run=false")
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

func checkFileHashExists(stmt *sql.Stmt, hash string, size int64) (bool, error) {
	var count int
	err := stmt.QueryRow(hash, size).Scan(&count)
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

	stmt, err := db.Prepare(`SELECT COUNT(*) FROM Files WHERE file_name = ?`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare SQL statement: %w", err)
	}
	defer stmt.Close()

	baseStmt, err := db.Prepare(`SELECT COUNT(*) FROM Files WHERE file_name LIKE ?`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare SQL statement: %w", err)
	}
	defer baseStmt.Close()

	hasStmt, err := db.Prepare(`SELECT COUNT(*) FROM Files WHERE file_hash=? AND file_size=?`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare SQL statement: %w", err)
	}
	defer hasStmt.Close()

	err = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("error accessing path %s: %w", path, err)
		}
		if d.IsDir() {
			return nil
		}

		if MediaType(path) != "image" && MediaType(path) != "video" {
			return nil
		}

		relPath, err := filepath.Rel(dir, path)
		if err != nil {
			return fmt.Errorf("failed to get relative path for %s: %w", path, err)
		}
		normalizedPath := strings.ReplaceAll(relPath, string(os.PathSeparator), "/")

		if !strings.HasPrefix(normalizedPath, "IPHONE") {
			return nil
		}

		exists, err := checkPathExists(stmt, normalizedPath)
		if err != nil {
			return fmt.Errorf("failed to check path %s: %w", normalizedPath, err)
		}

		if !exists {
			ext := filepath.Ext(normalizedPath)
			livePath := strings.TrimSuffix(normalizedPath, ext) + "%"

			exists, err = checkPathExists(baseStmt, livePath)
			if err != nil {
				return fmt.Errorf("failed to check live path %s: %w", livePath, err)
			}

			if !exists {
				hash, hErr := Hash(path)
				if hErr != nil {
					return hErr
				}
				info, _ := os.Stat(path)
				exists, err = checkFileHashExists(hasStmt, hash, info.Size())
				if err != nil {
					return fmt.Errorf("failed to check hash for %s: %w", normalizedPath, err)
				}
			}
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
	// Define flags
	configPath := flag.String("config", "./config.json", "Path to the JSON configuration file.")
	dbPath := flag.String("db", "", "Path to the SQLite database (override config).")
	thumbDir := flag.String("thumb-dir", "", "Path to the thumbnail directory (override config).")
	originDir := flag.String("origin-dir", "", "Path to the origin directory (override config).")
	dryRun := flag.Bool("dry-run", true, "Simulate deletion without actually removing files (override config).")
	help := flag.Bool("h", false, "Show help message.")
	flag.Parse()

	if *help {
		printHelp()
		return
	}

	// Load config
	config, err := LoadConfig(*configPath)
	if err != nil {
		fmt.Printf("Error loading config file: %v\n", err)
		return
	}

	// Override config with flags
	if *dbPath != "" {
		config.DB = *dbPath
	}
	if *thumbDir != "" {
		config.ThumbDir = *thumbDir
	}
	if *originDir != "" {
		config.OriginDir = *originDir
	}
	if dryRun != nil {
		config.DryRun = *dryRun
	}

	fmt.Printf("Using config: DB=%s, ThumbDir=%s, OriginDir=%s, DryRun=%t\n",
		config.DB, config.ThumbDir, config.OriginDir, config.DryRun)

	db, err := sql.Open("sqlite3", config.DB)
	if err != nil {
		fmt.Printf("Failed to connect to SQLite database: %v\n", err)
		return
	}
	defer db.Close()

	var totalFiles int
	thumbFiles := []string{}
	originFiles := []string{}

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

	if err := deleteFiles(thumbFiles, config.DryRun); err != nil {
		fmt.Printf("Error deleting thumbnails: %v\n", err)
	}
	if err := deleteFiles(originFiles, config.DryRun); err != nil {
		fmt.Printf("Error deleting originals: %v\n", err)
	}

	if totalFiles > 0 {
		fmt.Printf("\nTotal affected files: %d (thumbnails: %d, originals: %d)\n",
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