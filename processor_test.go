package benchmark

import (
	"database/sql"
	"fmt"
	"math/rand"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

const (
	parallelism = 36

	// dataSourceName = "root:123456@tcp(localhost:3306)/trade_service?collation=utf8_unicode_ci&charset=utf8mb4"
	dataSourceName = "portfolio:uYp-LSH-JK6-oHx@tcp(trading-bot-clone-auora-rds-4x-cluster.cmr8sblnu0ix.us-west-2.rds.amazonaws.com:3306)/trade_service?collation=utf8_unicode_ci&charset=utf8mb4"

	selectBuOrderBasicInfoSQL           = "SELECT * FROM `OrderStatusCenter_buorderbasicinfo` ORDER BY `id` DESC LIMIT ?"
	insertBuOrderBasicInfoSQL           = "INSERT INTO `OrderStatusCenter_buorderbasicinfo` (`bu_order_id`, `user_id`, `key_id`, `exchange`, `base`, `quote`, `bu_order_type`, `status`, `create_time`, `close_time`, `copy_from`, `copy_type`, `strategy_id`, `canceling`, `note`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	updateBuOrderBasicInfoSQL           = "UPDATE `OrderStatusCenter_buorderbasicinfo` SET `user_id`=?, `key_id`=?, `exchange`=?, `base`=?, `quote`=?, `bu_order_type`=?, `status`=?, `create_time`=?, `close_time`=?, `copy_from`=?, `copy_type`=?, `strategy_id`=?, `canceling`=?, `note`=? WHERE `bu_order_id`=?"
	updateBuOrderBasicInfoWithOutIdxSQL = "UPDATE `OrderStatusCenter_buorderbasicinfo` SET `status`=?, `create_time`=?, `close_time`=?, `canceling`=? WHERE `bu_order_id`=?"

	selectGridProOrderDataSQL = "SELECT * FROM `OrderStatusCenter_gridproorderdata` ORDER BY `id` DESC LIMIT ?"
	insertGridProOrderDataSQL = "INSERT INTO `OrderStatusCenter_gridproorderdata` (`bu_order_id`, `top`, `bottom`, `row`, `loss_stop`, `profit_stop`, `base_total_investment`, `quote_total_investment`, `grid_type`, `open_price`, `earn_coin`, `trend`, `init_price`, `init_quote_price`, `status`, `create_time`, `error`, `closed_quote_price`, `closed_price`, `base_investment`, `quote_investment`, `open_position_buy_limit`, `open_position_sell_limit`, `per_volume`, `init_base_amount`, `init_quote_amount`, `base_amount`, `quote_amount`, `placed_exchange_order_count`, `closed_exchange_order_count`, `exchange_order_paired_count`, `grid_profit`, `total_cost_in_base`, `total_cost_in_quote`, `total_fee_in_base`, `total_fee_in_quote`, `total_fee_refund_in_base`, `total_fee_refund_in_quote`, `paired_cost_in_base`, `paired_cost_in_quote`, `paired_fee_in_quote`, `paired_fee_refund_in_base`, `paired_fee_refund_in_quote`, `convert_into_earn_coin`, `paired_fee_in_base`, `reason_by`, `base_total_amount`, `blow_up_price`, `interest_amount`, `liquidate_direction`, `liquidate_price`, `liquidated_triggered`, `loan_amount`, `loan_coin`, `quote_total_amount`, `leverage`, `average_cost`, `base_fee_remain`, `base_fee_reserve`, `quote_fee_remain`, `quote_fee_reserve`, `condition`, `condition_direction`, `trigger_time`, `base_margin`, `quote_margin`, `risk_degree`, `risk_rate`, `init_base_margin`, `init_quote_margin`, `profit_withdrawn`, `fee_total_investment`, `grid_average_open_price`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	updateGridProOrderDataSQL = "UPDATE `OrderStatusCenter_gridproorderdata` SET `top`=?, `bottom`=?, `row`=?, `loss_stop`=?, `profit_stop`=?, `base_total_investment`=?, `quote_total_investment`=?, `grid_type`=?, `open_price`=?, `earn_coin`=?, `trend`=?, `init_price`=?, `init_quote_price`=?, `status`=?, `create_time`=?, `error`=?, `closed_quote_price`=?, `closed_price`=?, `base_investment`=?, `quote_investment`=?, `open_position_buy_limit`=?, `open_position_sell_limit`=?, `per_volume`=?, `init_base_amount`=?, `init_quote_amount`=?, `base_amount`=?, `quote_amount`=?, `placed_exchange_order_count`=?, `closed_exchange_order_count`=?, `exchange_order_paired_count`=?, `grid_profit`=?, `total_cost_in_base`=?, `total_cost_in_quote`=?, `total_fee_in_base`=?, `total_fee_in_quote`=?, `total_fee_refund_in_base`=?, `total_fee_refund_in_quote`=?, `paired_cost_in_base`=?, `paired_cost_in_quote`=?, `paired_fee_in_quote`=?, `paired_fee_refund_in_base`=?, `paired_fee_refund_in_quote`=?, `convert_into_earn_coin`=?, `paired_fee_in_base`=?, `reason_by`=?, `base_total_amount`=?, `blow_up_price`=?, `interest_amount`=?, `liquidate_direction`=?, `liquidate_price`=?, `liquidated_triggered`=?, `loan_amount`=?, `loan_coin`=?, `quote_total_amount`=?, `leverage`=?, `average_cost`=?, `base_fee_remain`=?, `base_fee_reserve`=?, `quote_fee_remain`=?, `quote_fee_reserve`=?, `condition`=?, `condition_direction`=?, `trigger_time`=?, `base_margin`=?, `quote_margin`=?, `risk_degree`=?, `risk_rate`=?, `init_base_margin`=?, `init_quote_margin`=?, `profit_withdrawn`=?, `fee_total_investment`=?, `grid_average_open_price`=? WHERE `bu_order_id`=?"

	selectExchangeOrderDataSQL = "SELECT * FROM `OrderStatusCenter_pairedexchangeorderdata_pionexv2_2020_09` ORDER BY `id` DESC LIMIT ?"
	insertExchangeOrderDataSQL = "INSERT INTO `OrderStatusCenter_pairedexchangeorderdata_pionexv2_2020_09` (`bu_order_id`, `tag`, `buy_exchange_order_id`, `buy_timestamp`, `buy_datetime`, `buy_lastTradeTimestamp`, `buy_symbol`, `buy_exchange`, `buy_base`, `buy_quote`, `buy_type`, `buy_side`, `buy_price`, `buy_average`, `buy_cost`, `buy_amount`, `buy_filled`, `buy_remaining`, `buy_status`, `buy_fee`, `buy_fee_coin`, `buy_info`, `buy_fee_income_cost`, `buy_fee_income_coin`, `buy_fee_in_quote`, `buy_fee_refund_in_quote`, `buy_fee_in_base`, `buy_fee_refund_in_base`, `buy_strategy_id`, `buy_strategy_tag`, `buy_tag`, `buy_client_order_id`, `sell_exchange_order_id`, `sell_timestamp`, `sell_datetime`, `sell_lastTradeTimestamp`, `sell_symbol`, `sell_exchange`, `sell_base`, `sell_quote`, `sell_type`, `sell_side`, `sell_price`, `sell_average`, `sell_cost`, `sell_amount`, `sell_filled`, `sell_remaining`, `sell_status`, `sell_fee`, `sell_fee_coin`, `sell_info`, `sell_fee_income_cost`, `sell_fee_income_coin`, `sell_fee_in_quote`, `sell_fee_refund_in_quote`, `sell_fee_in_base`, `sell_fee_refund_in_base`, `sell_strategy_id`, `sell_strategy_tag`, `sell_tag`, `sell_client_order_id`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	updateExchangeOrderDataSQL = "UPDATE `OrderStatusCenter_pairedexchangeorderdata_pionexv2_2020_09` SET (`tag`=?, `buy_exchange_order_id`=?, `buy_timestamp`=?, `buy_datetime`=?, `buy_lastTradeTimestamp`=?, `buy_symbol`=?, `buy_exchange`=?, `buy_base`=?, `buy_quote`=?, `buy_type`=?, `buy_side`=?, `buy_price`=?, `buy_average`=?, `buy_cost`=?, `buy_amount`=?, `buy_filled`=?, `buy_remaining`=?, `buy_status`=?, `buy_fee`=?, `buy_fee_coin`=?, `buy_info`=?, `buy_fee_income_cost`=?, `buy_fee_income_coin`=?, `buy_fee_in_quote`=?, `buy_fee_refund_in_quote`=?, `buy_fee_in_base`=?, `buy_fee_refund_in_base`=?, `buy_strategy_id`=?, `buy_strategy_tag`=?, `buy_tag`=?, `buy_client_order_id`=?, `sell_exchange_order_id`=?, `sell_timestamp`=?, `sell_datetime`=?, `sell_lastTradeTimestamp`=?, `sell_symbol`=?, `sell_exchange`=?, `sell_base`=?, `sell_quote`=?, `sell_type`=?, `sell_side`=?, `sell_price`=?, `sell_average`=?, `sell_cost`=?, `sell_amount`=?, `sell_filled`=?, `sell_remaining`=?, `sell_status`=?, `sell_fee`=?, `sell_fee_coin`=?, `sell_info`=?, `sell_fee_income_cost`=?, `sell_fee_income_coin`=?, `sell_fee_in_quote`=?, `sell_fee_refund_in_quote`=?, `sell_fee_in_base`=?, `sell_fee_refund_in_base`=?, `sell_strategy_id`=?, `sell_strategy_tag`=?, `sell_tag`=?, `sell_client_order_id`=? WHERE `bu_order_id`=?) WHERE `bu_order_id`=?"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type BuOrderBasicInfo struct {
	BuOrderId   string
	Id          int64
	UserId      sql.NullString
	KeyId       sql.NullString
	Exchange    sql.NullString
	Base        sql.NullString
	Quote       sql.NullString
	BuOrderType sql.NullString
	Status      sql.NullString
	CreateTime  sql.NullString
	CloseTime   sql.NullString
	CopyFrom    sql.NullString
	CopyType    sql.NullString
	StrategyId  sql.NullString
	Canceling   bool
	Note        sql.NullString
}

type GridProOrderData struct {
	Id                       int64
	BuOrderId                string
	Top                      sql.NullString
	Bottom                   sql.NullString
	Row                      sql.NullString
	LossStop                 sql.NullString
	ProfitStop               sql.NullString
	BaseTotalInvestment      sql.NullString
	QuoteTotalInvestment     sql.NullString
	GridType                 sql.NullString
	OpenPrice                sql.NullString
	EarnCoin                 sql.NullString
	Trend                    sql.NullString
	InitPrice                sql.NullString
	InitQuotePrice           sql.NullString
	Status                   sql.NullString
	CreateTime               sql.NullString
	Error                    sql.NullString
	ClosedQuotePrice         sql.NullString
	ClosedPrice              sql.NullString
	BaseInvestment           sql.NullString
	QuoteInvestment          sql.NullString
	OpenPositionBuyLimit     sql.NullString
	OpenPositionSellLimit    sql.NullString
	PerVolume                sql.NullString
	InitBaseAmount           sql.NullString
	InitQuoteAmount          sql.NullString
	BaseAmount               sql.NullString
	QuoteAmount              sql.NullString
	PlacedExchangeOrderCount int
	ClosedExchangeOrderCount int
	ExchangeOrderPairedCount int
	GridProfit               sql.NullString
	TotalCostInBase          sql.NullString
	TotalCostInQuote         sql.NullString
	TotalFeeInBase           sql.NullString
	TotalFeeInQuote          sql.NullString
	TotalFeeRefundInBase     sql.NullString
	TotalFeeRefundInQuote    sql.NullString
	PairedCostInBase         sql.NullString
	PairedCostInQuote        sql.NullString
	PairedFeeInQuote         sql.NullString
	PairedFeeRefundInBase    sql.NullString
	PairedFeeRefundInQuote   sql.NullString
	ConvertIntoEarnCoin      int
	PairedFeeInBase          sql.NullString
	ReasonBy                 sql.NullString
	BaseTotalAmount          sql.NullString
	BlowUpPrice              sql.NullString
	InterestAmount           sql.NullString
	LiquidateDirection       sql.NullString
	LiquidatePrice           sql.NullString
	LiquidatedTriggered      int
	LoanAmount               sql.NullString
	LoanCoin                 sql.NullString
	QuoteTotalAmount         sql.NullString
	Leverage                 sql.NullString
	AverageCost              sql.NullString
	BaseFeeRemain            sql.NullString
	BaseFeeReserve           sql.NullString
	QuoteFeeRemain           sql.NullString
	QuoteFeeReserve          sql.NullString
	Condition                sql.NullString
	ConditionDirection       sql.NullString
	TriggerTime              sql.NullString
	BaseMargin               sql.NullString
	QuoteMargin              sql.NullString
	RiskDegree               sql.NullString
	RiskRate                 sql.NullString
	InitBaseMargin           sql.NullString
	InitQuoteMargin          sql.NullString
	ProfitWithdrawn          sql.NullString
	FeeTotalInvestment       sql.NullString
	GridAverageOpenPrice     sql.NullString
}

type PairedExchangeOrderData struct {
	Id                     int64
	BuOrderId              string
	Tag                    sql.NullString
	BuyExchangeOrderId     sql.NullString
	BuyTimestamp           sql.NullString
	BuyDatetime            sql.NullString
	BuyLastTradeTimestamp  sql.NullString
	BuySymbol              sql.NullString
	BuyExchange            sql.NullString
	BuyBase                sql.NullString
	BuyQuote               sql.NullString
	BuyType                sql.NullString
	BuySide                sql.NullString
	BuyPrice               sql.NullString
	BuyAverage             sql.NullString
	BuyCost                sql.NullString
	BuyAmount              sql.NullString
	BuyFilled              sql.NullString
	BuyRemaining           sql.NullString
	BuyStatus              sql.NullString
	BuyFee                 sql.NullString
	BuyFeeCoin             sql.NullString
	BuyInfo                sql.NullString
	BuyFeeIncomeCost       sql.NullString
	BuyFeeIncomeCoin       sql.NullString
	BuyFeeInQuote          sql.NullString
	BuyFeeRefundInQuote    sql.NullString
	BuyFeeInBase           sql.NullString
	BuyFeeRefundInBase     sql.NullString
	BuyStrategyId          sql.NullString
	BuyStrategyTag         sql.NullString
	BuyTag                 sql.NullString
	BuyClientOrderId       sql.NullString
	SellExchangeOrderId    sql.NullString
	SellTimestamp          sql.NullString
	SellDatetime           sql.NullString
	SellLastTradeTimestamp sql.NullString
	SellSymbol             sql.NullString
	SellExchange           sql.NullString
	SellBase               sql.NullString
	SellQuote              sql.NullString
	SellType               sql.NullString
	SellSide               sql.NullString
	SellPrice              sql.NullString
	SellAverage            sql.NullString
	SellCost               sql.NullString
	SellAmount             sql.NullString
	SellFilled             sql.NullString
	SellRemaining          sql.NullString
	SellStatus             sql.NullString
	SellFee                sql.NullString
	SellFeeCoin            sql.NullString
	SellInfo               sql.NullString
	SellFeeIncomeCost      sql.NullString
	SellFeeIncomeCoin      sql.NullString
	SellFeeInQuote         sql.NullString
	SellFeeRefundInQuote   sql.NullString
	SellFeeInBase          sql.NullString
	SellFeeRefundInBase    sql.NullString
	SellStrategyId         sql.NullString
	SellStrategyTag        sql.NullString
	SellTag                sql.NullString
	SellClientOrderId      sql.NullString
}

func queryBuOrderBasicInfos(db *sql.DB) ([]BuOrderBasicInfo, error) {
	orders := make([]BuOrderBasicInfo, parallelism)
	rows, err := db.Query(selectBuOrderBasicInfoSQL, parallelism)
	if err != nil {
		return nil, err
	}
	defer rows.Next()

	i := 0
	for rows.Next() {
		o := &orders[i]
		err = rows.Scan(
			&o.BuOrderId,
			&o.Id,
			&o.UserId,
			&o.KeyId,
			&o.Exchange,
			&o.Base,
			&o.Quote,
			&o.BuOrderType,
			&o.Status,
			&o.CreateTime,
			&o.CloseTime,
			&o.CopyFrom,
			&o.CopyType,
			&o.StrategyId,
			&o.Canceling,
			&o.Note,
		)
		if err != nil {
			return nil, err
		}
		i += 1
	}
	return orders, rows.Err()
}

func queryGridProOrderData(db *sql.DB) ([]GridProOrderData, error) {
	orders := make([]GridProOrderData, parallelism)
	rows, err := db.Query(selectGridProOrderDataSQL, parallelism)
	if err != nil {
		return nil, err
	}
	defer rows.Next()

	i := 0
	for rows.Next() {
		o := &orders[i]
		err = rows.Scan(
			&o.Id,
			&o.BuOrderId,
			&o.Top,
			&o.Bottom,
			&o.Row,
			&o.LossStop,
			&o.ProfitStop,
			&o.BaseTotalInvestment,
			&o.QuoteTotalInvestment,
			&o.GridType,
			&o.OpenPrice,
			&o.EarnCoin,
			&o.Trend,
			&o.InitPrice,
			&o.InitQuotePrice,
			&o.Status,
			&o.CreateTime,
			&o.Error,
			&o.ClosedQuotePrice,
			&o.ClosedPrice,
			&o.BaseInvestment,
			&o.QuoteInvestment,
			&o.OpenPositionBuyLimit,
			&o.OpenPositionSellLimit,
			&o.PerVolume,
			&o.InitBaseAmount,
			&o.InitQuoteAmount,
			&o.BaseAmount,
			&o.QuoteAmount,
			&o.PlacedExchangeOrderCount,
			&o.ClosedExchangeOrderCount,
			&o.ExchangeOrderPairedCount,
			&o.GridProfit,
			&o.TotalCostInBase,
			&o.TotalCostInQuote,
			&o.TotalFeeInBase,
			&o.TotalFeeInQuote,
			&o.TotalFeeRefundInBase,
			&o.TotalFeeRefundInQuote,
			&o.PairedCostInBase,
			&o.PairedCostInQuote,
			&o.PairedFeeInQuote,
			&o.PairedFeeRefundInBase,
			&o.PairedFeeRefundInQuote,
			&o.ConvertIntoEarnCoin,
			&o.PairedFeeInBase,
			&o.ReasonBy,
			&o.BaseTotalAmount,
			&o.BlowUpPrice,
			&o.InterestAmount,
			&o.LiquidateDirection,
			&o.LiquidatePrice,
			&o.LiquidatedTriggered,
			&o.LoanAmount,
			&o.LoanCoin,
			&o.QuoteTotalAmount,
			&o.Leverage,
			&o.AverageCost,
			&o.BaseFeeRemain,
			&o.BaseFeeReserve,
			&o.QuoteFeeRemain,
			&o.QuoteFeeReserve,
			&o.Condition,
			&o.ConditionDirection,
			&o.TriggerTime,
			&o.BaseMargin,
			&o.QuoteMargin,
			&o.RiskDegree,
			&o.RiskRate,
			&o.InitBaseMargin,
			&o.InitQuoteMargin,
			&o.ProfitWithdrawn,
			&o.FeeTotalInvestment,
			&o.GridAverageOpenPrice,
		)
		if err != nil {
			return nil, err
		}
		i += 1
	}
	return orders, rows.Err()
}

func queryPairedExchangeOrderData(db *sql.DB) ([]PairedExchangeOrderData, error) {
	orders := make([]PairedExchangeOrderData, parallelism)
	rows, err := db.Query(selectExchangeOrderDataSQL, parallelism)
	if err != nil {
		return nil, err
	}
	defer rows.Next()

	i := 0
	for rows.Next() {
		o := &orders[i]
		err = rows.Scan(
			&o.Id,
			&o.BuOrderId,
			&o.Tag,
			&o.BuyExchangeOrderId,
			&o.BuyTimestamp,
			&o.BuyDatetime,
			&o.BuyLastTradeTimestamp,
			&o.BuySymbol,
			&o.BuyExchange,
			&o.BuyBase,
			&o.BuyQuote,
			&o.BuyType,
			&o.BuySide,
			&o.BuyPrice,
			&o.BuyAverage,
			&o.BuyCost,
			&o.BuyAmount,
			&o.BuyFilled,
			&o.BuyRemaining,
			&o.BuyStatus,
			&o.BuyFee,
			&o.BuyFeeCoin,
			&o.BuyInfo,
			&o.BuyFeeIncomeCost,
			&o.BuyFeeIncomeCoin,
			&o.BuyFeeInQuote,
			&o.BuyFeeRefundInQuote,
			&o.BuyFeeInBase,
			&o.BuyFeeRefundInBase,
			&o.BuyStrategyId,
			&o.BuyStrategyTag,
			&o.BuyTag,
			&o.BuyClientOrderId,
			&o.SellExchangeOrderId,
			&o.SellTimestamp,
			&o.SellDatetime,
			&o.SellLastTradeTimestamp,
			&o.SellSymbol,
			&o.SellExchange,
			&o.SellBase,
			&o.SellQuote,
			&o.SellType,
			&o.SellSide,
			&o.SellPrice,
			&o.SellAverage,
			&o.SellCost,
			&o.SellAmount,
			&o.SellFilled,
			&o.SellRemaining,
			&o.SellStatus,
			&o.SellFee,
			&o.SellFeeCoin,
			&o.SellInfo,
			&o.SellFeeIncomeCost,
			&o.SellFeeIncomeCoin,
			&o.SellFeeInQuote,
			&o.SellFeeRefundInQuote,
			&o.SellFeeInBase,
			&o.SellFeeRefundInBase,
			&o.SellStrategyId,
			&o.SellStrategyTag,
			&o.SellTag,
			&o.SellClientOrderId,
		)
		if err != nil {
			return nil, err
		}
		i += 1
	}
	return orders, rows.Err()
}

func updateBuOrderBasicInfo(tx *sql.Tx, buOrderId string, buOrder *BuOrderBasicInfo) error {
	_, err := tx.Exec(
		updateBuOrderBasicInfoSQL,
		buOrder.UserId,
		buOrder.KeyId,
		buOrder.Exchange,
		buOrder.Base,
		buOrder.Quote,
		buOrder.BuOrderType,
		buOrder.Status,
		buOrder.CreateTime,
		buOrder.CloseTime,
		buOrder.CopyFrom,
		buOrder.CopyType,
		buOrder.StrategyId,
		buOrder.Canceling,
		buOrder.Note,
		buOrderId,
	)
	return err
}

func updateGridProOrderData(tx *sql.Tx, buOrderId string, gridOrder *GridProOrderData) error {
	_, err := tx.Exec(
		updateGridProOrderDataSQL,
		gridOrder.Top,
		gridOrder.Bottom,
		gridOrder.Row,
		gridOrder.LossStop,
		gridOrder.ProfitStop,
		gridOrder.BaseTotalInvestment,
		gridOrder.QuoteTotalInvestment,
		gridOrder.GridType,
		gridOrder.OpenPrice,
		gridOrder.EarnCoin,
		gridOrder.Trend,
		gridOrder.InitPrice,
		gridOrder.InitQuotePrice,
		gridOrder.Status,
		gridOrder.CreateTime,
		gridOrder.Error,
		gridOrder.ClosedQuotePrice,
		gridOrder.ClosedPrice,
		gridOrder.BaseInvestment,
		gridOrder.QuoteInvestment,
		gridOrder.OpenPositionBuyLimit,
		gridOrder.OpenPositionSellLimit,
		gridOrder.PerVolume,
		gridOrder.InitBaseAmount,
		gridOrder.InitQuoteAmount,
		gridOrder.BaseAmount,
		gridOrder.QuoteAmount,
		gridOrder.PlacedExchangeOrderCount,
		gridOrder.ClosedExchangeOrderCount,
		gridOrder.ExchangeOrderPairedCount,
		gridOrder.GridProfit,
		gridOrder.TotalCostInBase,
		gridOrder.TotalCostInQuote,
		gridOrder.TotalFeeInBase,
		gridOrder.TotalFeeInQuote,
		gridOrder.TotalFeeRefundInBase,
		gridOrder.TotalFeeRefundInQuote,
		gridOrder.PairedCostInBase,
		gridOrder.PairedCostInQuote,
		gridOrder.PairedFeeInQuote,
		gridOrder.PairedFeeRefundInBase,
		gridOrder.PairedFeeRefundInQuote,
		gridOrder.ConvertIntoEarnCoin,
		gridOrder.PairedFeeInBase,
		gridOrder.ReasonBy,
		gridOrder.BaseTotalAmount,
		gridOrder.BlowUpPrice,
		gridOrder.InterestAmount,
		gridOrder.LiquidateDirection,
		gridOrder.LiquidatePrice,
		gridOrder.LiquidatedTriggered,
		gridOrder.LoanAmount,
		gridOrder.LoanCoin,
		gridOrder.QuoteTotalAmount,
		gridOrder.Leverage,
		gridOrder.AverageCost,
		gridOrder.BaseFeeRemain,
		gridOrder.BaseFeeReserve,
		gridOrder.QuoteFeeRemain,
		gridOrder.QuoteFeeReserve,
		gridOrder.Condition,
		gridOrder.ConditionDirection,
		gridOrder.TriggerTime,
		gridOrder.BaseMargin,
		gridOrder.QuoteMargin,
		gridOrder.RiskDegree,
		gridOrder.RiskRate,
		gridOrder.InitBaseMargin,
		gridOrder.InitQuoteMargin,
		gridOrder.ProfitWithdrawn,
		gridOrder.FeeTotalInvestment,
		gridOrder.GridAverageOpenPrice,
		buOrderId,
	)
	return err
}

func insertPairedExchangeOrderData(tx *sql.Tx, pairedOrder *PairedExchangeOrderData) error {
	buOrderId := uuid.New().String()
	_, err := tx.Exec(
		insertExchangeOrderDataSQL,
		buOrderId,
		pairedOrder.Tag,
		pairedOrder.BuyExchangeOrderId,
		pairedOrder.BuyTimestamp,
		pairedOrder.BuyDatetime,
		pairedOrder.BuyLastTradeTimestamp,
		pairedOrder.BuySymbol,
		pairedOrder.BuyExchange,
		pairedOrder.BuyBase,
		pairedOrder.BuyQuote,
		pairedOrder.BuyType,
		pairedOrder.BuySide,
		pairedOrder.BuyPrice,
		pairedOrder.BuyAverage,
		pairedOrder.BuyCost,
		pairedOrder.BuyAmount,
		pairedOrder.BuyFilled,
		pairedOrder.BuyRemaining,
		pairedOrder.BuyStatus,
		pairedOrder.BuyFee,
		pairedOrder.BuyFeeCoin,
		pairedOrder.BuyInfo,
		pairedOrder.BuyFeeIncomeCost,
		pairedOrder.BuyFeeIncomeCoin,
		pairedOrder.BuyFeeInQuote,
		pairedOrder.BuyFeeRefundInQuote,
		pairedOrder.BuyFeeInBase,
		pairedOrder.BuyFeeRefundInBase,
		pairedOrder.BuyStrategyId,
		pairedOrder.BuyStrategyTag,
		pairedOrder.BuyTag,
		pairedOrder.BuyClientOrderId,
		pairedOrder.SellExchangeOrderId,
		pairedOrder.SellTimestamp,
		pairedOrder.SellDatetime,
		pairedOrder.SellLastTradeTimestamp,
		pairedOrder.SellSymbol,
		pairedOrder.SellExchange,
		pairedOrder.SellBase,
		pairedOrder.SellQuote,
		pairedOrder.SellType,
		pairedOrder.SellSide,
		pairedOrder.SellPrice,
		pairedOrder.SellAverage,
		pairedOrder.SellCost,
		pairedOrder.SellAmount,
		pairedOrder.SellFilled,
		pairedOrder.SellRemaining,
		pairedOrder.SellStatus,
		pairedOrder.SellFee,
		pairedOrder.SellFeeCoin,
		pairedOrder.SellInfo,
		pairedOrder.SellFeeIncomeCost,
		pairedOrder.SellFeeIncomeCoin,
		pairedOrder.SellFeeInQuote,
		pairedOrder.SellFeeRefundInQuote,
		pairedOrder.SellFeeInBase,
		pairedOrder.SellFeeRefundInBase,
		pairedOrder.SellStrategyId,
		pairedOrder.SellStrategyTag,
		pairedOrder.SellTag,
		pairedOrder.SellClientOrderId,
	)
	return err
}

func processUpdateBuOrderBasicInfo(db *sql.DB, basicOrders []BuOrderBasicInfo) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	buOrderId := basicOrders[rand.Intn(parallelism)].BuOrderId
	buOrder := &basicOrders[rand.Intn(parallelism)]
	if _, err = tx.Exec(
		updateBuOrderBasicInfoSQL,
		buOrder.UserId,
		buOrder.KeyId,
		buOrder.Exchange,
		buOrder.Base,
		buOrder.Quote,
		buOrder.BuOrderType,
		buOrder.Status,
		buOrder.CreateTime,
		buOrder.CloseTime,
		buOrder.CopyFrom,
		buOrder.CopyType,
		buOrder.StrategyId,
		buOrder.Canceling,
		buOrder.Note,
		buOrderId,
	); err != nil {
		return err
	}
	err = tx.Commit()
	return err
}

func processUpdateGridProOrderData(db *sql.DB, gridOrders []GridProOrderData) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	buOrderId := gridOrders[rand.Intn(parallelism)].BuOrderId
	gridOrder := &gridOrders[rand.Intn(parallelism)]
	if _, err := tx.Exec(
		updateGridProOrderDataSQL,
		gridOrder.Top,
		gridOrder.Bottom,
		gridOrder.Row,
		gridOrder.LossStop,
		gridOrder.ProfitStop,
		gridOrder.BaseTotalInvestment,
		gridOrder.QuoteTotalInvestment,
		gridOrder.GridType,
		gridOrder.OpenPrice,
		gridOrder.EarnCoin,
		gridOrder.Trend,
		gridOrder.InitPrice,
		gridOrder.InitQuotePrice,
		gridOrder.Status,
		gridOrder.CreateTime,
		gridOrder.Error,
		gridOrder.ClosedQuotePrice,
		gridOrder.ClosedPrice,
		gridOrder.BaseInvestment,
		gridOrder.QuoteInvestment,
		gridOrder.OpenPositionBuyLimit,
		gridOrder.OpenPositionSellLimit,
		gridOrder.PerVolume,
		gridOrder.InitBaseAmount,
		gridOrder.InitQuoteAmount,
		gridOrder.BaseAmount,
		gridOrder.QuoteAmount,
		gridOrder.PlacedExchangeOrderCount,
		gridOrder.ClosedExchangeOrderCount,
		gridOrder.ExchangeOrderPairedCount,
		gridOrder.GridProfit,
		gridOrder.TotalCostInBase,
		gridOrder.TotalCostInQuote,
		gridOrder.TotalFeeInBase,
		gridOrder.TotalFeeInQuote,
		gridOrder.TotalFeeRefundInBase,
		gridOrder.TotalFeeRefundInQuote,
		gridOrder.PairedCostInBase,
		gridOrder.PairedCostInQuote,
		gridOrder.PairedFeeInQuote,
		gridOrder.PairedFeeRefundInBase,
		gridOrder.PairedFeeRefundInQuote,
		gridOrder.ConvertIntoEarnCoin,
		gridOrder.PairedFeeInBase,
		gridOrder.ReasonBy,
		gridOrder.BaseTotalAmount,
		gridOrder.BlowUpPrice,
		gridOrder.InterestAmount,
		gridOrder.LiquidateDirection,
		gridOrder.LiquidatePrice,
		gridOrder.LiquidatedTriggered,
		gridOrder.LoanAmount,
		gridOrder.LoanCoin,
		gridOrder.QuoteTotalAmount,
		gridOrder.Leverage,
		gridOrder.AverageCost,
		gridOrder.BaseFeeRemain,
		gridOrder.BaseFeeReserve,
		gridOrder.QuoteFeeRemain,
		gridOrder.QuoteFeeReserve,
		gridOrder.Condition,
		gridOrder.ConditionDirection,
		gridOrder.TriggerTime,
		gridOrder.BaseMargin,
		gridOrder.QuoteMargin,
		gridOrder.RiskDegree,
		gridOrder.RiskRate,
		gridOrder.InitBaseMargin,
		gridOrder.InitQuoteMargin,
		gridOrder.ProfitWithdrawn,
		gridOrder.FeeTotalInvestment,
		gridOrder.GridAverageOpenPrice,
		buOrderId,
	); err != nil {
		return err
	}

	err = tx.Commit()
	return err
}

func processInsertPairedExchangeOrderData(db *sql.DB, pairedOrders []PairedExchangeOrderData) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	buOrderId := uuid.New().String()
	pairedOrder := &pairedOrders[rand.Intn(parallelism)]
	if _, err := tx.Exec(
		insertExchangeOrderDataSQL,
		buOrderId,
		pairedOrder.Tag,
		pairedOrder.BuyExchangeOrderId,
		pairedOrder.BuyTimestamp,
		pairedOrder.BuyDatetime,
		pairedOrder.BuyLastTradeTimestamp,
		pairedOrder.BuySymbol,
		pairedOrder.BuyExchange,
		pairedOrder.BuyBase,
		pairedOrder.BuyQuote,
		pairedOrder.BuyType,
		pairedOrder.BuySide,
		pairedOrder.BuyPrice,
		pairedOrder.BuyAverage,
		pairedOrder.BuyCost,
		pairedOrder.BuyAmount,
		pairedOrder.BuyFilled,
		pairedOrder.BuyRemaining,
		pairedOrder.BuyStatus,
		pairedOrder.BuyFee,
		pairedOrder.BuyFeeCoin,
		pairedOrder.BuyInfo,
		pairedOrder.BuyFeeIncomeCost,
		pairedOrder.BuyFeeIncomeCoin,
		pairedOrder.BuyFeeInQuote,
		pairedOrder.BuyFeeRefundInQuote,
		pairedOrder.BuyFeeInBase,
		pairedOrder.BuyFeeRefundInBase,
		pairedOrder.BuyStrategyId,
		pairedOrder.BuyStrategyTag,
		pairedOrder.BuyTag,
		pairedOrder.BuyClientOrderId,
		pairedOrder.SellExchangeOrderId,
		pairedOrder.SellTimestamp,
		pairedOrder.SellDatetime,
		pairedOrder.SellLastTradeTimestamp,
		pairedOrder.SellSymbol,
		pairedOrder.SellExchange,
		pairedOrder.SellBase,
		pairedOrder.SellQuote,
		pairedOrder.SellType,
		pairedOrder.SellSide,
		pairedOrder.SellPrice,
		pairedOrder.SellAverage,
		pairedOrder.SellCost,
		pairedOrder.SellAmount,
		pairedOrder.SellFilled,
		pairedOrder.SellRemaining,
		pairedOrder.SellStatus,
		pairedOrder.SellFee,
		pairedOrder.SellFeeCoin,
		pairedOrder.SellInfo,
		pairedOrder.SellFeeIncomeCost,
		pairedOrder.SellFeeIncomeCoin,
		pairedOrder.SellFeeInQuote,
		pairedOrder.SellFeeRefundInQuote,
		pairedOrder.SellFeeInBase,
		pairedOrder.SellFeeRefundInBase,
		pairedOrder.SellStrategyId,
		pairedOrder.SellStrategyTag,
		pairedOrder.SellTag,
		pairedOrder.SellClientOrderId,
	); err != nil {
		return err
	}

	err = tx.Commit()
	return err
}

func process(db *sql.DB, basicOrders []BuOrderBasicInfo, gridOrders []GridProOrderData, pairedOrders []PairedExchangeOrderData, times int) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	j := rand.Intn(parallelism)
	for n := 0; n < times; n++ {
		i := rand.Intn(parallelism)
		buOrderId := basicOrders[j%parallelism].BuOrderId
		buOrder := &basicOrders[i]
		if _, err = tx.Exec(
			updateBuOrderBasicInfoSQL,
			buOrder.UserId,
			buOrder.KeyId,
			buOrder.Exchange,
			buOrder.Base,
			buOrder.Quote,
			buOrder.BuOrderType,
			buOrder.Status,
			buOrder.CreateTime,
			buOrder.CloseTime,
			buOrder.CopyFrom,
			buOrder.CopyType,
			buOrder.StrategyId,
			buOrder.Canceling,
			buOrder.Note,
			buOrderId,
		); err != nil {
			return err
		}

		gridOrder := &gridOrders[i]
		if _, err := tx.Exec(
			updateGridProOrderDataSQL,
			gridOrder.Top,
			gridOrder.Bottom,
			gridOrder.Row,
			gridOrder.LossStop,
			gridOrder.ProfitStop,
			gridOrder.BaseTotalInvestment,
			gridOrder.QuoteTotalInvestment,
			gridOrder.GridType,
			gridOrder.OpenPrice,
			gridOrder.EarnCoin,
			gridOrder.Trend,
			gridOrder.InitPrice,
			gridOrder.InitQuotePrice,
			gridOrder.Status,
			gridOrder.CreateTime,
			gridOrder.Error,
			gridOrder.ClosedQuotePrice,
			gridOrder.ClosedPrice,
			gridOrder.BaseInvestment,
			gridOrder.QuoteInvestment,
			gridOrder.OpenPositionBuyLimit,
			gridOrder.OpenPositionSellLimit,
			gridOrder.PerVolume,
			gridOrder.InitBaseAmount,
			gridOrder.InitQuoteAmount,
			gridOrder.BaseAmount,
			gridOrder.QuoteAmount,
			gridOrder.PlacedExchangeOrderCount,
			gridOrder.ClosedExchangeOrderCount,
			gridOrder.ExchangeOrderPairedCount,
			gridOrder.GridProfit,
			gridOrder.TotalCostInBase,
			gridOrder.TotalCostInQuote,
			gridOrder.TotalFeeInBase,
			gridOrder.TotalFeeInQuote,
			gridOrder.TotalFeeRefundInBase,
			gridOrder.TotalFeeRefundInQuote,
			gridOrder.PairedCostInBase,
			gridOrder.PairedCostInQuote,
			gridOrder.PairedFeeInQuote,
			gridOrder.PairedFeeRefundInBase,
			gridOrder.PairedFeeRefundInQuote,
			gridOrder.ConvertIntoEarnCoin,
			gridOrder.PairedFeeInBase,
			gridOrder.ReasonBy,
			gridOrder.BaseTotalAmount,
			gridOrder.BlowUpPrice,
			gridOrder.InterestAmount,
			gridOrder.LiquidateDirection,
			gridOrder.LiquidatePrice,
			gridOrder.LiquidatedTriggered,
			gridOrder.LoanAmount,
			gridOrder.LoanCoin,
			gridOrder.QuoteTotalAmount,
			gridOrder.Leverage,
			gridOrder.AverageCost,
			gridOrder.BaseFeeRemain,
			gridOrder.BaseFeeReserve,
			gridOrder.QuoteFeeRemain,
			gridOrder.QuoteFeeReserve,
			gridOrder.Condition,
			gridOrder.ConditionDirection,
			gridOrder.TriggerTime,
			gridOrder.BaseMargin,
			gridOrder.QuoteMargin,
			gridOrder.RiskDegree,
			gridOrder.RiskRate,
			gridOrder.InitBaseMargin,
			gridOrder.InitQuoteMargin,
			gridOrder.ProfitWithdrawn,
			gridOrder.FeeTotalInvestment,
			gridOrder.GridAverageOpenPrice,
			buOrderId,
		); err != nil {
			return err
		}

		buOrderId = uuid.New().String()
		pairedOrder := &pairedOrders[i]
		if _, err := tx.Exec(
			insertExchangeOrderDataSQL,
			buOrderId,
			pairedOrder.Tag,
			pairedOrder.BuyExchangeOrderId,
			pairedOrder.BuyTimestamp,
			pairedOrder.BuyDatetime,
			pairedOrder.BuyLastTradeTimestamp,
			pairedOrder.BuySymbol,
			pairedOrder.BuyExchange,
			pairedOrder.BuyBase,
			pairedOrder.BuyQuote,
			pairedOrder.BuyType,
			pairedOrder.BuySide,
			pairedOrder.BuyPrice,
			pairedOrder.BuyAverage,
			pairedOrder.BuyCost,
			pairedOrder.BuyAmount,
			pairedOrder.BuyFilled,
			pairedOrder.BuyRemaining,
			pairedOrder.BuyStatus,
			pairedOrder.BuyFee,
			pairedOrder.BuyFeeCoin,
			pairedOrder.BuyInfo,
			pairedOrder.BuyFeeIncomeCost,
			pairedOrder.BuyFeeIncomeCoin,
			pairedOrder.BuyFeeInQuote,
			pairedOrder.BuyFeeRefundInQuote,
			pairedOrder.BuyFeeInBase,
			pairedOrder.BuyFeeRefundInBase,
			pairedOrder.BuyStrategyId,
			pairedOrder.BuyStrategyTag,
			pairedOrder.BuyTag,
			pairedOrder.BuyClientOrderId,
			pairedOrder.SellExchangeOrderId,
			pairedOrder.SellTimestamp,
			pairedOrder.SellDatetime,
			pairedOrder.SellLastTradeTimestamp,
			pairedOrder.SellSymbol,
			pairedOrder.SellExchange,
			pairedOrder.SellBase,
			pairedOrder.SellQuote,
			pairedOrder.SellType,
			pairedOrder.SellSide,
			pairedOrder.SellPrice,
			pairedOrder.SellAverage,
			pairedOrder.SellCost,
			pairedOrder.SellAmount,
			pairedOrder.SellFilled,
			pairedOrder.SellRemaining,
			pairedOrder.SellStatus,
			pairedOrder.SellFee,
			pairedOrder.SellFeeCoin,
			pairedOrder.SellInfo,
			pairedOrder.SellFeeIncomeCost,
			pairedOrder.SellFeeIncomeCoin,
			pairedOrder.SellFeeInQuote,
			pairedOrder.SellFeeRefundInQuote,
			pairedOrder.SellFeeInBase,
			pairedOrder.SellFeeRefundInBase,
			pairedOrder.SellStrategyId,
			pairedOrder.SellStrategyTag,
			pairedOrder.SellTag,
			pairedOrder.SellClientOrderId,
		); err != nil {
			return err
		}
	}
	err = tx.Commit()
	return err
}

func BenchmarkOnlyUpdateBuOrderBasicInfo(b *testing.B) {
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	db.SetMaxOpenConns(parallelism)
	db.SetMaxIdleConns(parallelism / 2)
	db.SetConnMaxLifetime(120 * time.Second)

	basicOrders, err := queryBuOrderBasicInfos(db)
	if err != nil {
		b.Error(err.Error())
	}

	b.SetParallelism(parallelism)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := processUpdateBuOrderBasicInfo(db, basicOrders); err != nil {
				b.Error(err.Error())
			}
		}
	})
}

func BenchmarkOnlyUpdateGridProOrderData(b *testing.B) {
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	db.SetMaxOpenConns(parallelism)
	db.SetMaxIdleConns(parallelism / 2)
	db.SetConnMaxLifetime(120 * time.Second)

	gridOrders, err := queryGridProOrderData(db)
	if err != nil {
		b.Error(err.Error())
	}

	b.SetParallelism(parallelism)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := processUpdateGridProOrderData(db, gridOrders); err != nil {
				b.Error(err.Error())
			}
		}
	})
}

func BenchmarkOnlyInsertPairedExchangeOrderData(b *testing.B) {
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	db.SetMaxOpenConns(parallelism)
	db.SetMaxIdleConns(parallelism / 2)
	db.SetConnMaxLifetime(120 * time.Second)

	pairedOrders, err := queryPairedExchangeOrderData(db)
	if err != nil {
		b.Error(err.Error())
	}

	b.SetParallelism(parallelism)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := processInsertPairedExchangeOrderData(db, pairedOrders); err != nil {
				b.Error(err.Error())
			}
		}
	})
}

func BenchmarkProcess(b *testing.B) {
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	db.SetMaxOpenConns(parallelism)
	db.SetMaxIdleConns(parallelism / 2)
	db.SetConnMaxLifetime(120 * time.Second)

	basicOrders, err := queryBuOrderBasicInfos(db)
	if err != nil {
		b.Error(err.Error())
	}
	gridOrders, err := queryGridProOrderData(db)
	if err != nil {
		b.Error(err.Error())
	}
	pairedOrders, err := queryPairedExchangeOrderData(db)
	if err != nil {
		b.Error(err.Error())
	}

	b.SetParallelism(parallelism)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := process(db, basicOrders, gridOrders, pairedOrders, 1); err != nil {
				b.Error(err.Error())
			}
		}
	})
}

func BenchmarkProcessBatch10(b *testing.B) {
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	db.SetMaxOpenConns(parallelism)
	db.SetMaxIdleConns(parallelism / 2)
	db.SetConnMaxLifetime(120 * time.Second)

	basicOrders, err := queryBuOrderBasicInfos(db)
	if err != nil {
		b.Error(err.Error())
	}
	gridOrders, err := queryGridProOrderData(db)
	if err != nil {
		b.Error(err.Error())
	}
	pairedOrders, err := queryPairedExchangeOrderData(db)
	if err != nil {
		b.Error(err.Error())
	}

	b.SetParallelism(parallelism)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := process(db, basicOrders, gridOrders, pairedOrders, 1); err != nil {
				b.Error(err.Error())
			}
		}
	})
}

func BenchmarkProcessBatch50(b *testing.B) {
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	db.SetMaxOpenConns(parallelism)
	db.SetMaxIdleConns(parallelism / 2)
	db.SetConnMaxLifetime(120 * time.Second)

	basicOrders, err := queryBuOrderBasicInfos(db)
	if err != nil {
		b.Error(err.Error())
	}
	gridOrders, err := queryGridProOrderData(db)
	if err != nil {
		b.Error(err.Error())
	}
	pairedOrders, err := queryPairedExchangeOrderData(db)
	if err != nil {
		b.Error(err.Error())
	}

	b.SetParallelism(parallelism)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := process(db, basicOrders, gridOrders, pairedOrders, 50); err != nil {
				b.Error(err.Error())
			}
		}
	})
}

func BenchmarkProcessBatch100(b *testing.B) {
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	db.SetMaxOpenConns(parallelism)
	db.SetMaxIdleConns(parallelism / 2)
	db.SetConnMaxLifetime(120 * time.Second)

	basicOrders, err := queryBuOrderBasicInfos(db)
	if err != nil {
		b.Error(err.Error())
	}
	gridOrders, err := queryGridProOrderData(db)
	if err != nil {
		b.Error(err.Error())
	}
	pairedOrders, err := queryPairedExchangeOrderData(db)
	if err != nil {
		b.Error(err.Error())
	}

	b.SetParallelism(parallelism)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := process(db, basicOrders, gridOrders, pairedOrders, 100); err != nil {
				b.Error(err.Error())
			}
		}
	})
}

func BenchmarkProcessBatch1000(b *testing.B) {
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	db.SetMaxOpenConns(parallelism)
	db.SetMaxIdleConns(parallelism / 2)
	db.SetConnMaxLifetime(120 * time.Second)

	basicOrders, err := queryBuOrderBasicInfos(db)
	if err != nil {
		b.Error(err.Error())
	}
	gridOrders, err := queryGridProOrderData(db)
	if err != nil {
		b.Error(err.Error())
	}
	pairedOrders, err := queryPairedExchangeOrderData(db)
	if err != nil {
		b.Error(err.Error())
	}

	b.SetParallelism(parallelism)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := process(db, basicOrders, gridOrders, pairedOrders, 1000); err != nil {
				b.Error(err.Error())
			}
		}
	})
}

func BenchmarkHello(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fmt.Sprintf("hello")
	}
}
