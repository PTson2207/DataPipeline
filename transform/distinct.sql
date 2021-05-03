CREATE TABLE Orders (
    OrderId int,
    OrderStatus varchar(30),
    LastUpdated timestamp
);
INSERT INTO Orders
VALUES(1,'Backordered', '2020-06-01');
INSERT INTO Orders
VALUES(1,'Shipped', '2020-06-09');
INSERT INTO Orders
VALUES(2,'Shipped', '2020-07-11');
INSERT INTO Orders
VALUES(1,'Shipped', '2020-06-09');
INSERT INTO Orders
VALUES(3,'Shipped', '2020-07-12');


SELECT OrderId, OrderStatus, LastUpdated, COUNT(*) AS dup_count FROM Orders
GROUP BY OrderId, OrderStatus, LastUpdated
HAVING COUNT(*) > 1;