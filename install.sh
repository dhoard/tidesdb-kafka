#!/bin/bash
set -e

# TidesDB Kafka Streams Plugin Installer
# This script installs TidesDB native library and Kafka Streams plugin

VERSION="0.4.0"

# Source shared utilities
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/utils.sh"

print_box "" "TidesDB Kafka Streams Plugin Installer" "Version: $VERSION"
echo

# Fetch latest versions from GitHub
echo "Fetching latest TidesDB versions..."

if command -v curl &> /dev/null; then
    TIDESDB_CORE_VERSION=$(curl -s https://api.github.com/repos/tidesdb/tidesdb/releases/latest | grep '"tag_name":' | sed -E 's/.*"v([^"]+)".*/\1/')
    TIDESDB_JAVA_VERSION=$(curl -s https://api.github.com/repos/tidesdb/tidesdb-java/releases/latest | grep '"tag_name":' | sed -E 's/.*"v([^"]+)".*/\1/')
elif command -v wget &> /dev/null; then
    TIDESDB_CORE_VERSION=$(wget -qO- https://api.github.com/repos/tidesdb/tidesdb/releases/latest | grep '"tag_name":' | sed -E 's/.*"v([^"]+)".*/\1/')
    TIDESDB_JAVA_VERSION=$(wget -qO- https://api.github.com/repos/tidesdb/tidesdb-java/releases/latest | grep '"tag_name":' | sed -E 's/.*"v([^"]+)".*/\1/')
else
    echo "Neither curl nor wget found. Please install one of them."
    exit 1
fi

if [ -z "$TIDESDB_CORE_VERSION" ] || [ -z "$TIDESDB_JAVA_VERSION" ]; then
    echo "Failed to fetch latest versions. Using fallback versions..."
    TIDESDB_CORE_VERSION="8.5.3"
    TIDESDB_JAVA_VERSION="0.8.0"
fi

echo "✓ TidesDB Core: v$TIDESDB_CORE_VERSION"
echo "✓ TidesDB Java: v$TIDESDB_JAVA_VERSION"
echo

# Detect OS
OS="$(uname -s)"
ARCH="$(uname -m)"

if [ "$OS" = "Linux" ]; then
    if [ "$ARCH" = "x86_64" ]; then
        PLATFORM="linux-x86_64"
    elif [ "$ARCH" = "aarch64" ]; then
        PLATFORM="linux-aarch64"
    else
        echo "Unsupported architecture: $ARCH"
        exit 1
    fi
elif [ "$OS" = "Darwin" ]; then
    if [ "$ARCH" = "x86_64" ]; then
        PLATFORM="macos-x86_64"
    elif [ "$ARCH" = "arm64" ]; then
        PLATFORM="macos-arm64"
    else
        echo "Unsupported architecture: $ARCH"
        exit 1
    fi
else
    echo "Unsupported OS: $OS"
    exit 1
fi

echo "Detected platform: $PLATFORM"
echo

# Check dependencies
echo "Checking dependencies..."

if ! command -v java &> /dev/null; then
    echo "Java not found. Please install Java 11 or higher."
    exit 1
fi

JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1)
if [ "$JAVA_VERSION" -lt 11 ]; then
    echo "Java 11 or higher required. Found: Java $JAVA_VERSION"
    exit 1
fi

echo "✓ Java $JAVA_VERSION detected"

if ! command -v mvn &> /dev/null; then
    echo "⚠ Maven not found. Install Maven to build from source."
fi

# Check for build tools
if ! command -v cmake &> /dev/null; then
    echo "⚠ cmake not found. Please install cmake."
    echo "   Debian/Ubuntu: sudo apt-get install cmake"
    echo "   macOS: brew install cmake"
fi

if ! command -v make &> /dev/null; then
    echo "⚠ make not found. Please install build-essential (Debian/Ubuntu) or build tools."
fi

if ! command -v gcc &> /dev/null; then
    echo "⚠ gcc not found. Please install build-essential (Debian/Ubuntu) or build tools."
fi

echo

# Install TidesDB native library
echo "Building TidesDB native library from source..."

TEMP_DIR=$(mktemp -d)
cd "$TEMP_DIR"

echo "   Cloning TidesDB v${TIDESDB_CORE_VERSION}..."
if command -v git &> /dev/null; then
    git clone --depth 1 --branch "v${TIDESDB_CORE_VERSION}" https://github.com/tidesdb/tidesdb.git
    cd tidesdb
else
    echo "   Git not found, downloading source tarball..."
    if command -v curl &> /dev/null; then
        curl -L "https://github.com/tidesdb/tidesdb/archive/refs/tags/v${TIDESDB_CORE_VERSION}.tar.gz" -o tidesdb.tar.gz
    elif command -v wget &> /dev/null; then
        wget "https://github.com/tidesdb/tidesdb/archive/refs/tags/v${TIDESDB_CORE_VERSION}.tar.gz" -O tidesdb.tar.gz
    else
        echo "Neither git, curl, nor wget found. Please install one of them."
        exit 1
    fi
    tar -xzf tidesdb.tar.gz
    cd "tidesdb-${TIDESDB_CORE_VERSION}"
fi

echo "   Configuring with CMake..."
mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release

echo "   Building..."
make -j$(nproc 2>/dev/null || echo 2)

echo "   Installing library..."
if [ "$OS" = "Linux" ]; then
    sudo cp libtidesdb.so /usr/local/lib/
    sudo ldconfig
    echo "✓ TidesDB library installed to /usr/local/lib/libtidesdb.so"
elif [ "$OS" = "Darwin" ]; then
    sudo cp libtidesdb.dylib /usr/local/lib/
    echo "✓ TidesDB library installed to /usr/local/lib/libtidesdb.dylib"
fi

cd "$SCRIPT_DIR"
rm -rf "$TEMP_DIR"

echo

# Install TidesDB Java library to Maven local repository
echo "Installing TidesDB Java library to Maven..."

TEMP_DIR=$(mktemp -d)
cd "$TEMP_DIR"

echo "   Cloning TidesDB Java v${TIDESDB_JAVA_VERSION}..."
if command -v git &> /dev/null; then
    git clone --depth 1 --branch "v${TIDESDB_JAVA_VERSION}" https://github.com/tidesdb/tidesdb-java.git
    cd tidesdb-java
else
    echo "   Git not found, downloading source tarball..."
    if command -v curl &> /dev/null; then
        curl -L "https://github.com/tidesdb/tidesdb-java/archive/refs/tags/v${TIDESDB_JAVA_VERSION}.tar.gz" -o tidesdb-java.tar.gz
    elif command -v wget &> /dev/null; then
        wget "https://github.com/tidesdb/tidesdb-java/archive/refs/tags/v${TIDESDB_JAVA_VERSION}.tar.gz" -O tidesdb-java.tar.gz
    fi
    tar -xzf tidesdb-java.tar.gz
    cd "tidesdb-java-${TIDESDB_JAVA_VERSION}"
fi

# Build the JNI native library first
if [ -d "src/main/c" ]; then
    echo "   Building JNI native library..."
    cd src/main/c
    cmake -S . -B build
    cmake --build build
    sudo cmake --install build
    cd ../../..
    echo "✓ TidesDB JNI library installed"
else
    echo "⚠ JNI source directory not found, skipping JNI build"
fi

if command -v mvn &> /dev/null; then
    echo "   Building and installing to Maven local repository..."
    mvn clean install -DskipTests -Denforcer.skip=true -Dmaven.javadoc.skip=true
    echo "✓ TidesDB Java installed to Maven local repository"
else
    echo "⚠ Maven not found. Skipping TidesDB Java installation."
    echo "   You'll need to install it manually to run tests."
fi

cd "$SCRIPT_DIR"
rm -rf "$TEMP_DIR"

# Update library cache
if [ "$OS" = "Linux" ]; then
    sudo ldconfig
fi

echo

# Install Maven artifact
echo "Installing TidesDB Kafka Streams plugin..."

INSTALL_DIR="${INSTALL_DIR:-$HOME/.tidesdb-kafka}"
mkdir -p "$INSTALL_DIR"

if [ -f "target/tidesdb-kafka-streams-${VERSION}.jar" ]; then
    echo "   Found locally built JAR"
    cp "target/tidesdb-kafka-streams-${VERSION}.jar" "$INSTALL_DIR/"
else
    echo "   Downloading plugin JAR..."
    PLUGIN_URL="https://github.com/tidesdb/tidesdb-kafka-streams/releases/download/v${VERSION}/tidesdb-kafka-streams-${VERSION}.jar"
    
    if command -v curl &> /dev/null; then
        curl -L "$PLUGIN_URL" -o "$INSTALL_DIR/tidesdb-kafka-streams-${VERSION}.jar"
    else
        wget "$PLUGIN_URL" -O "$INSTALL_DIR/tidesdb-kafka-streams-${VERSION}.jar"
    fi
fi

echo "✓ Plugin installed to $INSTALL_DIR"

echo

echo "Creating configuration template..."

cat > "$INSTALL_DIR/tidesdb-kafka.properties" <<EOF
# TidesDB Kafka Streams Configuration
# Copy this file and customize as needed

# Database path (relative to Kafka Streams state.dir)
tidesdb.path=/tmp/kafka-streams-tidesdb

# Cache size (bytes, default: 64MB)
tidesdb.cache.size=67108864

# Number of flush threads (default: 2)
tidesdb.flush.threads=2

# Number of compaction threads (default: 2)
tidesdb.compaction.threads=2

# Log level (TRACE, DEBUG, INFO, WARN, ERROR)
tidesdb.log.level=INFO

# Write buffer size (bytes, default: 128MB)
tidesdb.write.buffer.size=134217728

# Compression algorithm (NONE, SNAPPY, LZ4, ZSTD, LZ4_FAST)
tidesdb.compression=LZ4

# Enable bloom filters (true/false)
tidesdb.bloom.filter.enabled=true

# Bloom filter false positive rate (0.0-1.0)
tidesdb.bloom.filter.fpr=0.01
EOF

echo "✓ Configuration template created: $INSTALL_DIR/tidesdb-kafka.properties"

echo

# Install to Maven local repository (if Maven is available)
if command -v mvn &> /dev/null && [ -f "pom.xml" ]; then
    echo "📦 Installing to Maven local repository..."
    mvn install -DskipTests
    echo "✓ Installed to Maven local repository"
    echo
fi

# Print usage instructions
print_box "" "Installation Complete!"
echo
echo "Quick Start:"
echo
echo "   1. Add to your pom.xml:"
echo "      <dependency>"
echo "          <groupId>com.tidesdb</groupId>"
echo "          <artifactId>tidesdb-kafka-streams</artifactId>"
echo "          <version>${VERSION}</version>"
echo "      </dependency>"
echo
echo "   2. Use in your Kafka Streams application:"
echo "      import com.tidesdb.kafka.store.TidesDBStoreBuilder;"
echo
echo "      Materialized.as("
echo "          new TidesDBStoreBuilder(\"my-store\")"
echo "              .withCachingEnabled()"
echo "      )"
echo
echo "Documentation: $INSTALL_DIR/README.md"
echo "Configuration: $INSTALL_DIR/tidesdb-kafka.properties"
echo "Plugin JAR: $INSTALL_DIR/tidesdb-kafka-streams-${VERSION}.jar"
echo
echo "Report issues: https://github.com/tidesdb/tidesdb-kafka/issues"
echo

echo "Verifying installation..."

if [ "$OS" = "Linux" ]; then
    if ldconfig -p | grep -q libtidesdb; then
        echo "✓ TidesDB library verified"
    else
        echo "TidesDB library not found in ldconfig cache"
    fi
elif [ "$OS" = "Darwin" ]; then
    if [ -f "/usr/local/lib/libtidesdb.dylib" ]; then
        echo "✓ TidesDB library verified"
    else
        echo "TidesDB library not found"
    fi
fi

if [ -f "$INSTALL_DIR/tidesdb-kafka-streams-${VERSION}.jar" ]; then
    echo "✓ Plugin JAR verified"
else
    echo "Plugin JAR not found"
fi

echo
echo "All done! Happy streaming with TidesDB!"
