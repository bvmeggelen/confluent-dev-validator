const {
	Kafka, logLevel,
	ErrorCodes, CompressionTypes, // You can specify additional optional properties for further configuration.
} = require('@confluentinc/kafka-javascript').KafkaJS;

const axios = require('axios'); // You may need to install axios: npm install axios
const avro = require('avsc'); // You may need to install avsc: npm install avsc
const fs = require('fs');
const path = require('path');

const samplesDirectory = 'samples';

/**
 * @description reads avsc and json files from samples directory.
 * Files in the samples/ directory must be named like 0000.[a-z].[json|avsc]
 *
 * Throws error if file could not be found
 *
 * @param id
 * @returns [{}, {}]
 */
function readSample(id) {
	const samplesDir = path.join(__dirname, 'samples');

	// Pad the ID with leading zeros to match the format (e.g., "1" becomes "0001")
	const paddedId = id.toString().padStart(4, '0');

	let jsonData = {};
	let avscSchema = {};

	try {
		// Read all files in the samples directory
		const files = fs.readdirSync(samplesDir);

		// Find matching files for this ID
		const jsonFile = files.find(file => {
			const pattern = new RegExp(`^${paddedId}\\.[a-z]+\\.json$`);
			return pattern.test(file);
		});

		const avscFile = files.find(file => {
			const pattern = new RegExp(`^${paddedId}\\.[a-z]+\\.avsc$`);
			return pattern.test(file);
		});

		if (!jsonFile) {
			throw new Error(`JSON file not found for ID ${id} (expected pattern: ${paddedId}.[a-z].json)`);
		}

		if (!avscFile) {
			throw new Error(`AVSC file not found for ID ${id} (expected pattern: ${paddedId}.[a-z].avsc)`);
		}

		// Read JSON file
		const jsonPath = path.join(samplesDir, jsonFile);
		const jsonContent = fs.readFileSync(jsonPath, 'utf8');
		try {
			jsonData = JSON.parse(jsonContent);
		} catch (parseError) {
			throw new Error(`Failed to parse JSON file ${jsonFile}: ${parseError.message}`);
		}

		// Read AVSC file
		const avscPath = path.join(samplesDir, avscFile);
		const avscContent = fs.readFileSync(avscPath, 'utf8');
		try {
			avscSchema = JSON.parse(avscContent);
		} catch (parseError) {
			throw new Error(`Failed to parse AVSC file ${avscFile}: ${parseError.message}`);
		}
	} catch (error) {
		if (error.code === 'ENOENT' && error.path === samplesDir) {
			throw new Error(`Samples directory not found: ${samplesDir}`);
		}
		throw error;
	}

	return [jsonData, avscSchema];
}

const producer = new Kafka().producer({
	'bootstrap.servers': 'broker:29092',
	log_level: logLevel.WARN,
});

// Schema Registry client configuration
const SCHEMA_REGISTRY_URL = 'http://schema-registry:8081';

/**
 * Custom error class for detailed schema validation errors
 */
class SchemaValidationError extends Error {
	constructor(message, details) {
		super(message);
		this.name = 'SchemaValidationError';
		this.details = details;
	}
}

/**
 * Fetch the latest schema for a given topic
 * @param {string} topic - The Kafka topic name
 * @param {string} type - 'key' or 'value'
 * @returns {Promise<Object>} Schema information
 */
async function getLatestSchema(topic, type = 'value') {
	try {
		const subject = `${topic}-${type}`;
		const response = await axios.get(`${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions/latest`);
		return response.data;
	} catch (error) {
		if (error.response && error.response.status === 404) {
			console.warn(`No schema found for subject: ${topic}-${type}`);
			return null;
		}
		throw error;
	}
}

/**
 * Get schema by ID with detailed information
 * @param {number} schemaId - The schema ID to fetch
 * @returns {Promise<Object>} Schema information
 */
async function getSchemaById(schemaId) {
	try {
		const response = await axios.get(`${SCHEMA_REGISTRY_URL}/schemas/ids/${schemaId}`);
		return response.data;
	} catch (error) {
		if (error.response && error.response.status === 404) {
			throw new SchemaValidationError(`Schema ID ${schemaId} not found in Schema Registry`, {
				schemaId,
				errorType: 'SCHEMA_NOT_FOUND',
				registryUrl: SCHEMA_REGISTRY_URL
			});
		}
		throw error;
	}
}

/**
 * Parse and validate Avro schema
 * @param {string} schemaString - The Avro schema as string
 * @returns {Object} Parsed Avro schema
 */
function parseAvroSchema(schemaString) {
	try {
		return avro.Type.forSchema(JSON.parse(schemaString));
	} catch (error) {
		throw new SchemaValidationError('Failed to parse Avro schema', {
			errorType: 'SCHEMA_PARSE_ERROR',
			originalError: error.message,
			schema: schemaString
		});
	}
}

/**
 * Validate a single message against an Avro schema
 * @param {Object} message - The raw message object to validate
 * @param {Object} avroSchema - The parsed Avro schema
 * @param {number} messageIndex - Index of the message in the batch
 * @returns {Object} Validation result
 */
function validateMessage(message, avroSchema, messageIndex) {
	try {
		// Validate against Avro schema
		const isValid = avroSchema.isValid(message);

		if (!isValid) {
			// Try to get more specific error information
			try {
				avroSchema.fromBuffer(avroSchema.toBuffer(message));
			} catch (validationError) {
				return {
					valid: false,
					error: {
						messageIndex,
						message,
						validationError: validationError.message,
						expectedSchema: avroSchema.toString(),
						actualType: typeof message,
						actualValue: message
					}
				};
			}
		}

		return { valid: true };
	} catch (error) {
		return {
			valid: false,
			error: {
				messageIndex,
				message,
				validationError: error.message,
				expectedSchema: avroSchema.toString(),
				actualType: typeof message,
				actualValue: message,
				errorType: 'VALIDATION_ERROR'
			}
		};
	}
}

/**
 * Validate messages against topic schema with detailed error reporting
 * @param {string} topic - The Kafka topic name
 * @param {Array} messages - Array of message objects to validate
 * @returns {Promise<Object>} Detailed validation result
 */
async function validateMessages(topic, messages) {
	const validationResult = {
		valid: true,
		topic,
		totalMessages: messages.length,
		validatedMessages: 0,
		errors: [],
		warnings: [],
		schemaInfo: {}
	};

	try {
		// Get schemas for the topic (only checking value schema since messages are the values)
		const valueSchema = await getLatestSchema(topic, 'value');

		if (!valueSchema) {
			validationResult.warnings.push({
				type: 'NO_SCHEMA_FOUND',
				message: `No value schema found for topic: ${topic}. Validation skipped.`,
				topic
			});
			return validationResult;
		}

		// Parse value schema
		let parsedValueSchema = null;

		validationResult.schemaInfo.value = {
			id: valueSchema.id,
			version: valueSchema.version,
			subject: valueSchema.subject
		};

		try {
			const valueSchemaDetails = await getSchemaById(valueSchema.id);
			parsedValueSchema = parseAvroSchema(valueSchemaDetails.schema);
			validationResult.schemaInfo.value.schema = valueSchemaDetails.schema;
		} catch (error) {
			validationResult.errors.push({
				type: 'VALUE_SCHEMA_ERROR',
				message: `Failed to parse value schema: ${error.message}`,
				schemaId: valueSchema.id,
				details: error.details || {}
			});
			validationResult.valid = false;
			return validationResult;
		}

		// Validate each message
		for (let i = 0; i < messages.length; i++) {
			const message = messages[i];

			// Validate the entire message object against the value schema
			const messageValidation = validateMessage(message, parsedValueSchema, i);
			if (!messageValidation.valid) {
				validationResult.errors.push({
					type: 'MESSAGE_VALIDATION_ERROR',
					message: `Message ${i} validation failed`,
					...messageValidation.error
				});
				validationResult.valid = false;
			}

			validationResult.validatedMessages++;
		}

		return validationResult;

	} catch (error) {
		validationResult.valid = false;
		validationResult.errors.push({
			type: 'GENERAL_VALIDATION_ERROR',
			message: `Schema validation failed for topic ${topic}: ${error.message}`,
			originalError: error.message,
			stack: error.stack
		});
		return validationResult;
	}
}

/**
 * Pretty print validation results
 * @param {Object} validationResult - The validation result object
 */
function printValidationResults(validationResult) {
	console.log('\n========== SCHEMA VALIDATION RESULTS ==========');
	console.log(`Topic: ${validationResult.topic}`);
	console.log(`Status: ${validationResult.valid ? '✅ PASSED' : '❌ FAILED'}`);
	console.log(`Messages Validated: ${validationResult.validatedMessages}/${validationResult.totalMessages}`);

	if (validationResult.schemaInfo.value) {
		console.log(`\n📄 Value Schema Info:`);
		console.log(`  - ID: ${validationResult.schemaInfo.value.id}`);
		console.log(`  - Version: ${validationResult.schemaInfo.value.version}`);
		console.log(`  - Subject: ${validationResult.schemaInfo.value.subject}`);
	}

	if (validationResult.warnings?.length > 0) {
		console.log(`\n⚠️  WARNINGS (${validationResult.warnings.length}):`);
		validationResult.warnings.forEach((warning, index) => {
			console.log(`  ${index + 1}. ${warning.message}`);
			console.log(`     Type: ${warning.type}`);
		});
	}

	if (validationResult.errors?.length > 0) {
		console.log(`\n❌ ERRORS (${validationResult.errors.length}):`);
		validationResult.errors.forEach((error, index) => {
			console.log(`\n  ${index + 1}. ${JSON.stringify(error.message)}`);
			console.log(`     Type: ${error.type}`);

			if (error.messageIndex !== undefined) {
				console.log(`     Message Index: ${error.messageIndex}`);
			}

			if (error.actualValue !== undefined) {
				console.log(`     Actual Value: ${JSON.stringify(error.actualValue)}`);
				console.log(`     Actual Type: ${error.actualType}`);
			}

			if (error.expectedSchema) {
				console.log(`     Expected Schema: ${error.expectedSchema}`);
			}

			if (error.validationError) {
				console.log(`     Validation Error: ${error.validationError}`);
			}

			if (error.details) {
				console.log(`     Additional Details: ${JSON.stringify(error.details, null, 2)}`);
			}
		});
	}

	console.log('\n===============================================\n');
}

/**
 * Encode message using Avro schema for Confluent Schema Registry
 * @param {Object} message - The message object to encode
 * @param {Object} avroSchema - The parsed Avro schema
 * @param {number} schemaId - The schema ID from Schema Registry
 * @returns {Buffer} Encoded message buffer
 */
function encodeAvroMessage(message, avroSchema, schemaId) {
	try {
		// Serialize the message using Avro schema
		const avroBuffer = avroSchema.toBuffer(message);

		// Create Confluent Schema Registry format:
		// Magic byte (0x0) + Schema ID (4 bytes) + Avro serialized data
		const magicByte = Buffer.alloc(1, 0);
		const schemaIdBuffer = Buffer.alloc(4);
		schemaIdBuffer.writeInt32BE(schemaId, 0);

		return Buffer.concat([magicByte, schemaIdBuffer, avroBuffer]);
	} catch (error) {
		throw new SchemaValidationError(`Failed to encode message with Avro schema: ${error.message}`, {
			message,
			schemaId,
			errorType: 'AVRO_ENCODING_ERROR',
			originalError: error.message
		});
	}
}

/**
 * Validate and encode messages for Kafka sending
 * @param {string} topic - The Kafka topic name
 * @param {Array} messages - Array of message objects
 * @returns {Promise<Array>} Array of encoded Kafka messages
 */
async function validateAndEncodeMessages(topic, messages) {
	// First validate the messages
	const validationResult = await validateMessages(topic, messages);

	// Print detailed validation results
	printValidationResults(validationResult);

	// If validation failed, throw error
	if (!validationResult.valid) {
		throw new SchemaValidationError('Schema validation failed', validationResult);
	}

	// If no schema found, send as JSON strings
	if (!validationResult.schemaInfo.value) {
		console.log('No schema found, sending as JSON strings...');
		return messages.map((message, index) => ({
			value: JSON.stringify(message),
			key: index.toString() // Simple key generation
		}));
	}

	// Get the schema details for encoding
	const valueSchemaId = validationResult.schemaInfo.value.id;
	const valueSchemaDetails = await getSchemaById(valueSchemaId);
	const parsedValueSchema = parseAvroSchema(valueSchemaDetails.schema);

	// Encode messages with Avro
	const encodedMessages = messages.map((message, index) => {
		const encodedValue = encodeAvroMessage(message, parsedValueSchema, valueSchemaId);

		return {
			value: encodedValue,
			key: index.toString() // Simple key generation
		};
	});

	console.log(`Successfully encoded ${encodedMessages.length} messages with Avro schema ID ${valueSchemaId}`);
	return encodedMessages;
}

/**
 * Pretty print delivery reports with detailed information
 * @param {Array} deliveryReports - Array of delivery report objects
 * @param {string} topic - The topic name
 * @param {number} messageCount - Number of messages sent
 */
function printDeliveryReports(deliveryReports, topic, messageCount) {
	console.log('\n========== DELIVERY REPORTS ==========');
	console.log(`Topic: ${topic}`);
	console.log(`Messages Sent: ${messageCount}`);
	console.log(`Reports Received: ${deliveryReports.length}`);

	if (deliveryReports.length === 0) {
		console.log('❌ No delivery reports received');
		console.log('=====================================\n');
		return;
	}

	// Calculate summary statistics
	const successful = deliveryReports.filter(report => !report.error).length;
	const failed = deliveryReports.filter(report => report.error).length;
	const partitions = [...new Set(deliveryReports.map(report => report.partition))];

	console.log(`✅ Successful: ${successful}`);
	console.log(`❌ Failed: ${failed}`);
	console.log(`📂 Partitions Used: ${partitions.join(', ')}`);

	// Show detailed information for each message
	console.log('\n📋 Message Details:');
	deliveryReports.forEach((report, index) => {
		const status = report.error ? '❌' : '✅';

		console.log(`\n  ${status} Message ${index + 1}:`);
		console.log(`     Partition: ${report.partition}`);
		console.log(`     Offset: ${report.offset}`);

		if (report.key !== undefined) {
			console.log(`     Key: ${report.key}`);
		}

		if (report.error) {
			console.log(`     Error: ${report.error.message || report.error}`);
			if (report.error.code) {
				console.log(`     Error Code: ${report.error.code}`);
			}
		} else {
			console.log(`     Size: ${report.size || 'N/A'} bytes`);
		}
	});

	// Show timing information if available
	const timestamps = deliveryReports
		.filter(report => report.timestamp)
		.map(report => report.timestamp);

	if (timestamps.length > 1) {
		const minTime = Math.min(...timestamps);
		const maxTime = Math.max(...timestamps);
		const duration = maxTime - minTime;

		console.log(`\n⏱️  Timing Information:`);
		console.log(`     First Message: ${new Date(minTime).toISOString()}`);
		console.log(`     Last Message: ${new Date(maxTime).toISOString()}`);
		console.log(`     Duration: ${duration}ms`);
	}

	console.log('\n=====================================\n');
}

/**
 * Validate messages against a registered schema in Schema Registry
 * @param {string} topic - The Kafka topic name
 * @param {Array} messages - Array of message objects
 * @param {number} schemaId - The registered schema ID
 * @returns {Promise<Object>} Validation result
 */
async function validateMessagesAgainstRegisteredSchema(topic, messages, schemaId) {
	const validationResult = {
		valid: true,
		schemaId,
		totalMessages: messages.length,
		validatedMessages: 0,
		errors: [],
		schemaInfo: {}
	};

	try {
		// Get the schema from Schema Registry
		const schemaDetails = await getSchemaById(schemaId);
		const parsedSchema = parseAvroSchema(schemaDetails.schema);

		validationResult.schemaInfo = {
			id: schemaId,
			schema: JSON.parse(schemaDetails.schema),
			subject: schemaDetails.subject || `${topic}-value`
		};

		// Validate each message
		for (let i = 0; i < messages.length; i++) {
			const message = messages[i];
			const messageValidation = validateMessage(message, parsedSchema, i);

			if (!messageValidation.valid) {
				validationResult.errors.push({
					type: 'MESSAGE_VALIDATION_ERROR',
					message: `Message ${i} validation failed against registered schema ${schemaId}`,
					...messageValidation.error
				});
				validationResult.valid = false;
			}

			validationResult.validatedMessages++;
		}

		return validationResult;

	} catch (error) {
		validationResult.valid = false;
		validationResult.errors.push({
			type: 'SCHEMA_REGISTRY_ERROR',
			message: `Failed to validate against registered schema ${schemaId}: ${error.message}`,
			originalError: error.message
		});
		return validationResult;
	}
}

/**
 * Register a schema from a sample file to Schema Registry
 * @param {string} topic - The Kafka topic name
 * @param {number|string} sampleId - The sample ID to load
 * @param {string} type - 'key' or 'value'
 * @returns {Promise<Object>} Schema registration result with { id, subject }
 */
async function registerSchemaFromSample(topic, sampleId, type = 'value') {
	const [, avscSchema] = readSample(sampleId);
	const subject = `${topic}-${type}`;

	console.log(`📤 Registering schema for subject: ${subject}`);

	try {
		const response = await axios.post(
			`${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions`,
			{ schema: JSON.stringify(avscSchema) },
			{ headers: { 'Content-Type': 'application/vnd.schemaregistry.v1+json' } }
		);

		console.log(`✅ Schema registered with ID: ${response.data.id}`);
		return { id: response.data.id, subject };

	} catch (error) {
		handleSchemaRegistryError(error, subject);
	}
}

/**
 * Register schema and validate messages in one step
 * @param {string} topic - The Kafka topic name
 * @param {number|string} sampleId - The sample ID to load
 * @param {Array} messages - Messages to validate
 * @param {string} type - 'key' or 'value'
 * @returns {Promise<Object>} Combined registration and validation result
 */
async function registerSchemaAndValidateMessages(topic, sampleId, messages, type = 'value') {
	console.log(`🔄 Processing sample ${sampleId} for topic: ${topic}`);

	// Load and validate schema format
	const [, avscSchema] = readSample(sampleId);
	validateSchemaFormat(avscSchema, sampleId);

	// Register schema to Schema Registry
	const { id: schemaId, subject } = await registerSchemaFromSample(topic, sampleId, type);

	// Validate messages against registered schema
	console.log(`🔍 Validating ${messages.length} message(s) against registered schema...`);
	const validationResult = await validateMessagesAgainstRegisteredSchema(topic, messages, schemaId);

	// Add registration metadata
	validationResult.registrationInfo = {
		schemaId,
		sampleId,
		subject,
		registeredAt: new Date().toISOString()
	};

	return validationResult;
}

/**
 * Validate that a schema can be parsed as valid Avro
 * @param {Object} avscSchema - The schema object to validate
 * @param {string|number} sampleId - Sample ID for error reporting
 */
function validateSchemaFormat(avscSchema, sampleId) {
	try {
		parseAvroSchema(JSON.stringify(avscSchema));
		console.log('✅ Schema format is valid');
	} catch (parseError) {
		throw new SchemaValidationError(`Invalid Avro schema in sample ${sampleId}`, {
			errorType: 'SCHEMA_PARSE_ERROR',
			sampleId,
			originalError: parseError.message
		});
	}
}

/**
 * Handle Schema Registry API errors with descriptive messages
 * @param {Error} error - The axios error
 * @param {string} subject - The schema subject name
 */
function handleSchemaRegistryError(error, subject) {
	if (!error.response) {
		if (error.code === 'ECONNREFUSED') {
			throw new Error(`Cannot connect to Schema Registry at ${SCHEMA_REGISTRY_URL}. Is it running?`);
		}
		throw new Error(`Network error during schema registration: ${error.message}`);
	}

	const { status, data } = error.response;
	const errorMessage = data.message || data.error_code || error.response.statusText;

	switch (status) {
		case 409:
			console.log(`ℹ️  Schema already exists for subject: ${subject}`);
			throw new Error(`Schema conflict: ${errorMessage}`);
		case 422:
			throw new Error(`Invalid schema format: ${errorMessage}`);
		case 401:
			throw new Error(`Authentication failed: ${errorMessage}`);
		case 403:
			throw new Error(`Authorization failed: ${errorMessage}`);
		default:
			throw new Error(`Schema Registry error (${status}): ${errorMessage}`);
	}
}
(async () => {
	await producer.connect();

	try {
		const topic = 'debug';
		const sampleId = 1; // Use sample 0001.example.*

		// Load sample data
		const [sampleJsonData, avscSchema] = readSample(sampleId);

		// Use sample data or custom messages
		const messages = [sampleJsonData];

		// Register schema and validate messages - this is the critical step
		console.log('\n🚀 Starting schema registration and validation process...');
		const validationResult = await registerSchemaAndValidateMessages(topic, sampleId, messages, 'value');

		// Print validation results
		printValidationResults(validationResult);

		// Only continue if schema registration and validation succeeded
		if (!validationResult.valid) {
			throw new SchemaValidationError('Schema registration or message validation failed', validationResult);
		}

		console.log(`\n✅ Schema registered and messages validated successfully!`);
		console.log(`   Schema ID: ${validationResult.registrationInfo.schemaId}`);
		console.log(`   Subject: ${validationResult.registrationInfo.subject}`);

		// Now encode and send messages using the registered schema
		console.log('\n📦 Encoding messages with registered schema...');
		const schemaId = validationResult.registrationInfo.schemaId;
		const schemaDetails = await getSchemaById(schemaId);
		const parsedSchema = parseAvroSchema(schemaDetails.schema);

		const encodedMessages = messages.map((message, index) => {
			const encodedValue = encodeAvroMessage(message, parsedSchema, schemaId);
			return {
				value: encodedValue,
				key: index.toString()
			};
		});

		console.log(`✅ Successfully encoded ${encodedMessages.length} messages`);

		// Send messages
		console.log('\n🚀 Sending messages to Kafka...');
		const deliveryReports = await producer.send({
			topic,
			messages: encodedMessages
		});

		printDeliveryReports(deliveryReports, topic, encodedMessages.length);

		await producer.disconnect();

	} catch (e) {
		if (e instanceof SchemaValidationError) {
			console.error('\n🚨 SCHEMA OPERATION FAILED 🚨');
			console.error('Error:', e.message);
			// if (e.details) {
			// 	console.error('Details:', JSON.stringify(e.details, null, 2));
			// }
		} else {
			console.error('General error:', e);
		}
		await producer.disconnect();
		process.exit(1); // Exit with error code since schema registration failed
	}
})();
