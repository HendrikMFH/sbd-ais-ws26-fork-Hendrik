package rest

import (
	"encoding/json"
	"net/http"
	"ordersystem/model"
	"ordersystem/repository"

	"github.com/go-chi/render"
)

// GetMenu 			godoc
// @tags 			Menu
// @Description 	Returns the menu of all drinks
// @Produce  		json
// @Success 		200 {array} model.Drink
// @Router 			/api/menu [get]
func GetMenu(db *repository.DatabaseHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// todo
		// get slice from db
		// render.Status(r, http.StatusOK)
		// render.JSON(w, r, <your-slice>)
		drinks := db.GetDrinks()
		render.Status(r, http.StatusOK)
		render.JSON(w, r, drinks)
	}
}

// todo create GetOrders /api/order/all
// GetOrders		godoc
// @tags 			orders
// @Description 	Returns all orders
// @Produce  		json
// @Success 		200 {array} model.Order
// @Router 			/api/orders/all [get]
func GetOrders(db *repository.DatabaseHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		orders := db.GetOrders()
		render.Status(r, http.StatusOK)
		render.JSON(w, r, orders)
	}
}
// todo create GetOrdersTotal /api/order/total
// GetOrdersTotal	godoc
// @tags 			TotalOrders
// @Description 	Returns amount of orders for each drink
// @Produce  		json
// @Success 		200 {object} map[string]uint64 
// @Router 			/api/orders/total [get]
func GetOrdersTotal(db *repository.DatabaseHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		totalOrders := db.GetTotalledOrders()
		render.Status(r, http.StatusOK)
		render.JSON(w, r, totalOrders)
	}
}

// PostOrder 		godoc
// @tags 			Order
// @Description 	Adds an order to the db
// @Accept 			json
// @Param 			b body model.Order true "Order"
// @Produce  		json
// @Success 		200
// @Failure     	400
// @Router 			/api/order [post]
func PostOrder(db *repository.DatabaseHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// todo
		// declare empty order struct
		// err := json.NewDecoder(r.Body).Decode(&<your-order-struct>)
		// handle error and render Status 400
		// add to db
		var order model.Order

		err:= json.NewDecoder(r.Body).Decode(&order)
		if err != nil {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, err.Error())
			return
		}
		db.AddOrder(&order)
		render.Status(r, http.StatusOK)
		render.JSON(w, r, "ok")
	}
}
