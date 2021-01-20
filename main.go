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
	usrRepo := NewUserRepo(db)

	for {
		var cmd string
		fmt.Print("Введите \"ececute\": ")
		fmt.Fscan(os.Stdin, &cmd)

		switch cmd {
		case "money":
			ShowMoney(trRepo)
		case "activations": {
			var email string
			fmt.Print("Введите \"email\" пользователя: ")
			fmt.Fscan(os.Stdin, &email)
			err := ShowUserActivations(usrRepo, email)
			if err != nil {
				panic(err)
			}
		}
		default:
			fmt.Println("Неизвестная команда")
			continue
		}
	}
}

func ShowMoney(trRepo *TransactionRepo) error {
	byDay := make(map[string][]Transaction)
	days := make([]string, 0)

	list := trRepo.Get()

	for _, tr := range list {
		dt, err := time.Parse(time.RFC3339, tr.CreatedAt)
		if err != nil {
			return err
		}

		dKey := hashDate(dt)

		if _, ok := byDay[dKey]; !ok {
			days = append(days, dKey)
		}

		byDay[dKey] = append(byDay[dKey], tr)
	}

	printStatistic(byDay, days)

	return nil
}

func ShowUserActivations(usrRepo *UserRepo, email string) error {
	usr, err := usrRepo.GetByEmail(email)
	if err != nil {
		return err
	}

	fmt.Printf("user: %+v", usr)

	return nil
}


func hashDate(dt time.Time) string {
	y, m, d := dt.Date()
	return fmt.Sprintf("%d_%v_%v", y, m.String(), formatDay(d))
}

func formatDay(day int) string {
	if (day < 10) {
		return fmt.Sprintf("0%d", day)
	}

	return fmt.Sprintf("%d", day)
}

func printStatistic(m map[string][]Transaction, days []string) {
	table := make([][]string, 0, len(days))
	table = append(table, []string{"Day", "Count", "Pays", "Buys", "Day Balance"})

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

type User struct {
	ID string
	Email string
}

type UserRepo struct {
	DB *sql.DB
}

func NewUserRepo(db *sql.DB) *UserRepo {
	return &UserRepo{db}
}

func (r *UserRepo) GetByEmail(e string) (*User, error) {
	sqlStatement := `SELECT id, email FROM "user" WHERE email = $1`

	user := User{}
	rows := r.DB.QueryRow(sqlStatement, e)
	err := rows.Scan(&user.ID, &user.Email)
	if err != nil {
		return nil, err 
	}

	return &user, nil
}