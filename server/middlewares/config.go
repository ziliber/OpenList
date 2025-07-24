package middlewares

import (
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// UnifiedFilteredLogger returns a filtered logger using global configuration
// serverType: "http" for main HTTP server, "s3" for S3 server
func UnifiedFilteredLogger(serverType string) gin.HandlerFunc {
	config := conf.Conf.Log.Filter
	
	if !config.EnableFiltering {
		// Return standard Gin logger if filtering is disabled
		return gin.LoggerWithWriter(log.StandardLogger().Out)
	}
	
	loggerConfig := FilteredLoggerConfig{
		Output: log.StandardLogger().Out,
	}
	
	// Add health check paths
	if config.FilterHealthChecks {
		loggerConfig.SkipPaths = append(loggerConfig.SkipPaths, "/ping")
	}
	
	// Add HEAD method filtering
	if config.FilterHEADRequests {
		loggerConfig.SkipMethods = append(loggerConfig.SkipMethods, "HEAD")
	}
	
	// Add WebDAV filtering only for HTTP server (not for S3)
	if config.FilterWebDAV && serverType == "http" {
		loggerConfig.SkipPathPrefixes = append(loggerConfig.SkipPathPrefixes, "/dav/")
		loggerConfig.SkipMethods = append(loggerConfig.SkipMethods, "PROPFIND")
	}
	
	// Add custom configurations
	loggerConfig.SkipPaths = append(loggerConfig.SkipPaths, config.CustomSkipPaths...)
	loggerConfig.SkipMethods = append(loggerConfig.SkipMethods, config.CustomSkipMethods...)
	loggerConfig.SkipPathPrefixes = append(loggerConfig.SkipPathPrefixes, config.CustomSkipPrefixes...)
	
	return FilteredLoggerWithConfig(loggerConfig)
}

// HTTPFilteredLogger returns a filtered logger for the main HTTP server
func HTTPFilteredLogger() gin.HandlerFunc {
	return UnifiedFilteredLogger("http")
}

// S3FilteredLogger returns a filtered logger for the S3 server
func S3FilteredLogger() gin.HandlerFunc {
	return UnifiedFilteredLogger("s3")
}