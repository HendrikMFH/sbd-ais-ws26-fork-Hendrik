package repository

import (
	"ordersystem/model"
	"time"
)

type DatabaseHandler struct {
	// drinks represent all available drinks
	drinks []model.Drink
	// orders serves as order history
	orders []model.Order
}

// todo
func NewDatabaseHandler() *DatabaseHandler {
	// Init the drinks slice with some test data
	// drinks := ...
	drinks := []model.Drink{
		{ID: 1, Name: "Cola", Price: 2.5, Description:"Soft drink with caffeine"},
		{ID: 2, Name: "Water", Price: 1.5, Description:"Still water"},
		{ID: 3, Name: "Coffee", Price: 3.0, Description: "Black coffee"},
	}
	// Init orders slice with some test data
	orders := []model.Order{
		{DrinkID: 1, CreatedAt: time.Now(), Amount: 2},
		{DrinkID: 2, CreatedAt: time.Now(), Amount: 1},
	}

	return &DatabaseHandler{
		drinks: drinks,
		orders: orders,
	}
}

func (db *DatabaseHandler) GetDrinks() []model.Drink {
	return db.drinks
}

func (db *DatabaseHandler) GetOrders() []model.Order {
	return db.orders
}

// todo
func (db *DatabaseHandler) GetTotalledOrders() map[uint64]uint64 {
	// calculate total orders
	// key = DrinkID, value = Amount of orders
	// totalledOrders map[uint64]uint64
	totalledOrders := make(map[uint64]uint64)

	for _, order := range db.orders {
		totalledOrders[order.DrinkID] += order.Amount
	}

	return totalledOrders
}

func (db *DatabaseHandler) AddOrder(order *model.Order) {
	// todo
	// add order to db.orders slice
	db.orders = append(db.orders, *order)
}
