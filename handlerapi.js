const db = require("./db");
const {
  GetItemCommand,
  PutItemCommand,
  DeleteItemCommand,
  ScanCommand,
  UpdateItemCommand,
  BatchWriteItemCommand, // ✅ Added for bulk insert
} = require("@aws-sdk/client-dynamodb");
const { marshall, unmarshall } = require("@aws-sdk/util-dynamodb");

// Helper: format success & error responses
const formatResponse = (statusCode, message, data = {}, error = null) => {
  return {
    statusCode,
    body: JSON.stringify({
      message,
      ...(error && { error }),
      ...(data && { data }),
    }),
  };
};

// Helper: parse input safely
const safeParse = (input) => {
  try {
    return JSON.parse(input);
  } catch {
    return null;
  }
};

// ✅ GET a single post by postId
const getPost = async (event) => {
  const postId = event?.pathParameters?.postId;

  if (!postId) {
    return formatResponse(400, "Missing postId in path.");
  }

  try {
    const params = {
      TableName: process.env.DYNAMODB_TABLE_NAME,
      Key: marshall({ postId }),
    };

    const { Item } = await db.send(new GetItemCommand(params));
    return formatResponse(200, "getPost retrieved successfully.", {
      post: Item ? unmarshall(Item) : null,
      rawData: Item,
    });
  } catch (err) {
    return formatResponse(500, "Error retrieving post.", null, err.message);
  }
};

// ✅ CREATE a new post
const createPost = async (event) => {
  const body = safeParse(event.body);
  if (!body || !body.postId) {
    return formatResponse(400, "Missing required post fields.");
  }

  try {
    const params = {
      TableName: process.env.DYNAMODB_TABLE_NAME,
      Item: marshall(body),
    };

    const result = await db.send(new PutItemCommand(params));
    return formatResponse(201, "Post created successfully.", { result });
  } catch (err) {
    return formatResponse(500, "Error creating post.", null, err.message);
  }
};

// ✅ CREATE MULTIPLE posts at once
const createMultiplePosts = async (event) => {
  const posts = safeParse(event.body);

  if (!Array.isArray(posts) || posts.length === 0) {
    return formatResponse(400, "Request body must be a non-empty array of post objects.");
  }

  try {
    const putRequests = posts.map((post) => ({
      PutRequest: {
        Item: marshall(post),
      },
    }));

    const params = {
      RequestItems: {
        [process.env.DYNAMODB_TABLE_NAME]: putRequests,
      },
    };

    const result = await db.send(new BatchWriteItemCommand(params));

    const unprocessed = result.UnprocessedItems?.[process.env.DYNAMODB_TABLE_NAME] || [];

    return formatResponse(201, "Bulk insert completed.", {
      inserted: posts.length - unprocessed.length,
      unprocessedItems: unprocessed.map((item) => unmarshall(item.PutRequest.Item)),
    });
  } catch (err) {
    return formatResponse(500, "Error inserting multiple posts.", null, err.message);
  }
};

// ✅ UPDATE a post by ID
const updatePost = async (event) => {
  const postId = event?.pathParameters?.postId;
  const body = safeParse(event.body);

  if (!postId || !body || Object.keys(body).length === 0) {
    return formatResponse(400, "Invalid update request.");
  }

  const keys = Object.keys(body);
  const UpdateExpression = `SET ${keys.map((_, i) => `#k${i} = :v${i}`).join(", ")}`;
  const ExpressionAttributeNames = Object.fromEntries(keys.map((k, i) => [`#k${i}`, k]));
  const ExpressionAttributeValues = marshall(
    Object.fromEntries(keys.map((k, i) => [`:v${i}`, body[k]]))
  );

  try {
    const params = {
      TableName: process.env.DYNAMODB_TABLE_NAME,
      Key: marshall({ postId }),
      UpdateExpression,
      ExpressionAttributeNames,
      ExpressionAttributeValues,
    };

    const result = await db.send(new UpdateItemCommand(params));
    return formatResponse(200, "Post updated successfully.", { result });
  } catch (err) {
    return formatResponse(500, "Error updating post.", null, err.message);
  }
};

// ✅ DELETE a post
const deletePost = async (event) => {
  const postId = event?.pathParameters?.postId;
  if (!postId) return formatResponse(400, "Missing postId.");

  try {
    const params = {
      TableName: process.env.DYNAMODB_TABLE_NAME,
      Key: marshall({ postId }),
    };

    const result = await db.send(new DeleteItemCommand(params));
    return formatResponse(200, "Post deleted successfully.", { result });
  } catch (err) {
    return formatResponse(500, "Error deleting post.", null, err.message);
  }
};

// ✅ GET all posts
const getAllPosts = async () => {
  try {
    const { Items } = await db.send(
      new ScanCommand({ TableName: process.env.DYNAMODB_TABLE_NAME })
    );

    const posts = Items.map((item) => unmarshall(item));
    return formatResponse(200, "All posts retrieved.", { posts });
  } catch (err) {
    return formatResponse(500, "Failed to get posts.", null, err.message);
  }
};

module.exports = {
  getPost,
  createPost,
  createMultiplePosts, // ✅ Newly added function
  updatePost,
  deletePost,
  getAllPosts,
};
