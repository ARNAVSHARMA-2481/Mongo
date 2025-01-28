// Insert one document
db.inv.insertOne({
  item: "notebook", 
  qty: 50,
  price: 5.99,
  category: "stationery",
  supplier: {
    name: "Office Supplies Inc.",
    contact: "office@supplier.com",
    location: "New York"
  },
  tags: ["office", "writing"],
  inStock: true,
  createdAt: new Date()
});

// Aggregation query: Group by genre and count the number of books
db.books.aggregate([
  { $group: { _id: "$genre", count: { $sum: 1 } } }
]);

// Insert many documents
db.inv.insertMany([
  {
    item: "pen",
    qty: 200,
    price: 1.5,
    category: "stationery",
    supplier: {
      name: "PenWorld",
      contact: "support@penworld.com",
      location: "Chicago"
    },
    tags: ["writing", "office"],
    inStock: true,
    createdAt: new Date()
  },
  {
    item: "stapler",
    qty: 30,
    price: 12.99,
    category: "office supplies",
    supplier: {
      name: "Office Supplies Inc.",
      contact: "office@supplier.com",
      location: "New York"
    },
    tags: ["office", "tools"],
    inStock: true,
    createdAt: new Date()
  },
  {
    item: "eraser",
    qty: 150,
    price: 0.99,
    category: "stationery",
    supplier: {
      name: "School Basics Co.",
      contact: "info@schoolbasics.com",
      location: "San Francisco"
    },
    tags: ["school", "writing"],
    inStock: true,
    createdAt: new Date()
  }
]);

// Read all documents in the collection
db.inv.find().pretty();

// Read a single document
db.inv.findOne({ item: "pen" });

// Find multiple items by category
db.inv.find({ category: "stationery" }).pretty();

// Use logical operators in queries
// Find items with qty greater than 100
db.inv.find({ qty: { $gt: 100 } }).pretty();

// Project specific fields (include item and price, exclude _id)
db.inv.find({}, { item: 1, price: 1, _id: 0 }).pretty();

// Exclude specific fields (exclude supplier)
db.inv.find({}, { supplier: 0 }).pretty();

// Use operators for complex queries

// Match multiple conditions using $and
// Find stationery items with qty greater than 50
db.inv.find({
  $and: [
    { category: "stationery" },
    { qty: { $gt: 50 } }
  ]
}).pretty();

// OR condition with $or
// Find items that are either stationery or have a qty less than 100
db.inv.find({
  $or: [
    { category: "stationery" },
    { qty: { $lt: 100 } }
  ]
}).pretty();

// Sorting and limiting results

// Sort by price in ascending order
db.inv.find().sort({ price: 1 }).pretty();

// Limit the number of results to 2
db.inv.find().limit(2).pretty();

// Combine sort and limit
// Sort by qty in descending order and limit to 3 results
db.inv.find().sort({ qty: -1 }).limit(3).pretty();

// Count the number of documents with a specific condition
db.inv.countDocuments({ inStock: true });

// Aggregation: Group items by category and calculate total quantity
db.inv.aggregate([
  { $group: { _id: "$category", totalQty: { $sum: "$qty" } } }
]);

// Indexing
// Create an index on the item field
db.inv.createIndex({ item: 1 });

// Query using the index and explain the execution plan
db.inv.find({ item: "pen" }).explain("executionStats");

// Create a compound index on category and price fields
db.inv.createIndex({ category: 1, price: -1 });

// Aggregation framework for data analysis

// Match and group items by category, calculating total qty
db.inv.aggregate([
  { $match: { qty: { $gt: 50 } } },
  { $group: { _id: "$category", totalQty: { $sum: "$qty" } } }
]);

// Sort in the aggregation pipeline
db.inv.aggregate([
  { $match: { qty: { $gt: 50 } } },
  { $group: { _id: "$category", totalQty: { $sum: "$qty" } } },
  { $sort: { totalQty: -1 } }
]);

// Project specific fields in aggregation
db.inv.aggregate([
  { $project: { item: 1, price: 1, discountedPrice: { $multiply: ["$price", 0.9] } } }
]);

// Transactions for multi-document operations
const session = db.getMongo().startSession();
session.startTransaction();

try {
  session.getDatabase("test").inv.updateOne({ item: "pen" }, { $set: { qty: 150 } }, { session });
  session.getDatabase("test").inv.insertOne({ item: "ruler", qty: 75, price: 2.5 }, { session });
  session.commitTransaction();
} catch (error) {
  session.abortTransaction();
  throw error;
} finally {
  session.endSession();
}

// Insert multiple authors and books
db.authors.insertMany([
  { _id: 1, name: "J.K. Rowling", age: 55, country: "UK" },
  { _id: 2, name: "George R.R. Martin", age: 74, country: "USA" },
  { _id: 3, name: "J.R.R. Tolkien", age: 127, country: "South Africa" },
  { _id: 4, name: "Suzanne Collins", age: 61, country: "USA" }
]);

db.books.insertMany([
  { _id: 101, title: "Harry Potter and the Philosopher's Stone", authorId: 1, genre: "Fantasy" },
  { _id: 102, title: "Harry Potter and the Chamber of Secrets", authorId: 1, genre: "Fantasy" },
  { _id: 103, title: "A Game of Thrones", authorId: 2, genre: "Fantasy" },
  { _id: 104, title: "A Clash of Kings", authorId: 2, genre: "Fantasy" },
  { _id: 105, title: "The Hobbit", authorId: 3, genre: "Adventure" },
  { _id: 106, title: "The Lord of the Rings", authorId: 3, genre: "Adventure" },
  { _id: 107, title: "The Hunger Games", authorId: 4, genre: "Dystopian" }
]);

// CRUD: Insert an author and a book
db.authors.insertOne({
  _id: 5,
  name: "Rick Riordan",
  age: 59,
  country: "USA"
});

db.books.insertOne({
  _id: 108,
  title: "Percy Jackson: The Lightning Thief",
  authorId: 5,
  genre: "Fantasy"
});

// READ: Query books by authorId
db.books.find({ authorId: 1 }); // Displays all books written by J.K. Rowling

// Find author by name and retrieve their books
const author = db.authors.findOne({ name: "J.K. Rowling" });
db.books.find({ authorId: author._id }); // Find all books by J.K. Rowling

// Aggregate: Join books with authors and display details
db.books.aggregate([
  {
    $lookup: {
      from: "authors",
      localField: "authorId",
      foreignField: "_id",
      as: "authorDetails"
    }
  }
]);

// Update author details
db.authors.updateOne(
  { _id: 2 }, // George R.R. Martin
  { $set: { country: "United Kingdom", age: 75 } }
);

// Update book genre
db.books.updateOne(
  { _id: 103 }, // A Game of Thrones
  { $set: { genre: "Epic Fantasy" } }
);

// Reassign a book to a different author
db.books.updateOne(
  { _id: 107 }, // The Hunger Games
  { $set: { authorId: 1 } } // Reassign to J.K. Rowling
);

// Delete operations: Remove a book and its author
db.books.deleteOne({ _id: 108 }); // Deletes Percy Jackson: The Lightning Thief

// Delete all books by a specific author, then delete the author
db.books.deleteMany({ authorId: 3 }); // Deletes all books by J.R.R. Tolkien
db.authors.deleteOne({ _id: 3 }); // Deletes J.R.R. Tolkien

// Create index on authorId for faster queries
db.books.createIndex({ authorId: 1 });

// Aggregate to count books by genre
db.books.aggregate([
  { $group: { _id: "$genre", count: { $sum: 1 } } }
]);

// Skip and limit results in queries
db.books.find().skip(0).limit(3); // Skip first 0 documents and limit to 3

