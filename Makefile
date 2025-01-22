# Default file path
FILE_PATH_CREDENTIALS ?= libnet-d76db-firebase-adminsdk-ju1ex-d1382d36b4.json
FILE_PATH_CONFIG ?= firestoreConfig.json
build:
	@echo "Building the project..."

run:
	@echo "Running the project..."

clean:
	@echo "Cleaning the project..."

fly_secrets:
	@echo "Setting secrets from file..."
	flyctl secrets set FIREBASE_CREDENTIALS=$(cat $(FILE_PATH_CREDENTIALS) | base64)
	flyctl secrets set FIRESTORE_CONFIG=$(cat $(FILE_PATH_CONFIG) | base64)
	@echo "Secrets set successfully!"
