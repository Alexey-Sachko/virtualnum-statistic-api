package main

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func main () {
	db, err := sql.Open("postgres", getDSN())
	err = db.Ping() // вот тут будет первое подключение к базе
	if err != nil {
		panic(err)
	}

	trRepo := NewTransactionRepo(db)

	

	byDay := make(map[string][]Transaction)
	days := make([]string, 0)

	list := trRepo.Get()

	for _, tr := range list {
		// fmt.Printf("%+v\n", tr)
		dt, err := time.Parse(time.RFC3339, tr.CreatedAt)
		if err != nil {
			panic(err)
		}

		dKey := hashDate(dt)

		if _, ok := byDay[dKey]; !ok {
			days = append(days, dKey)
		}

		byDay[dKey] = append(byDay[dKey], tr)
	}

	printStatistic(byDay, days)
}

func hashDate(dt time.Time) string {
	y, m, d := dt.Date()
	return fmt.Sprintf("%d_%v_%d", y, m.String(), d)
}

func printStatistic(m map[string][]Transaction, days []string) {
	total := 0

	for _, day := range days {
		transactions, ok := m[day]
		if !ok {
			panic("not found in map key: " + day)
		}

		count := len(transactions)
		payAmount := 0
		buyAmount := 0
		dayTotal := 0

		for _, tr := range transactions {
			if tr.Amount > 0 {
				payAmount += tr.Amount
			} else {
				buyAmount += tr.Amount * -1
			}

			dayTotal += tr.Amount
		}

		total += dayTotal
		fmt.Printf("%v | %v | %v | %v | %v\n", day, count, toDecimal(payAmount), toDecimal(buyAmount), toDecimal(dayTotal))
	}

	fmt.Println("\nTotal users balance: ", toDecimal(total))
}

func toDecimal(amount int) float32 {
	return float32(amount) / 100
}

func getDSN() string {
	err := godotenv.Load()
	if err != nil {
		panic(err)
	}

	host := os.Getenv("DB_HOST")
	user := os.Getenv("DB_USERNAME")
	pass := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_DATABASE")
	port := os.Getenv("DB_PORT")

	return fmt.Sprintf("user=%v password=%v host=%v port=%v dbname=%v", user, pass, host, port, dbName)
}

type Transaction struct {
	ID string
	Amount int
	BalanceBefore int
	CreatedAt string
	UserID string
	Type string
}

type TransactionRepo struct {
	DB *sql.DB
}

func NewTransactionRepo(db *sql.DB) *TransactionRepo {
	return &TransactionRepo{db}
}

func (r *TransactionRepo) Get() []Transaction {
	sqlStatement := `SELECT id, amount, "balanceBefore", "createdAt", "userId", type FROM "transaction" ORDER BY "createdAt" DESC`

	rows, err := r.DB.Query(sqlStatement)
	if err != nil {
		panic(err)
	}

	result := make([]Transaction, 0)

	for rows.Next() {
		t := Transaction{}
		err := rows.Scan(&t.ID, &t.Amount, &t.BalanceBefore, &t.CreatedAt, &t.UserID, &t.Type)
		if err != nil {
			panic(err)
		}

		result = append(result, t)
	}

	return result
}