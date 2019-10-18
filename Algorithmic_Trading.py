import alpaca_trade_api as tradeapi
import threading
import time
import datetime
import statistics as stat
import numpy as np
import pandas as pd

API_KEY = "CHANGE_ME"
API_SECRET = "CHANGE_ME"
APCA_API_BASE_URL = "https://paper-api.alpaca.markets"
alpaca = tradeapi.REST(API_KEY, API_SECRET, APCA_API_BASE_URL, 'v2')



class LongShort:
  def __init__(self):
    self.alpaca = tradeapi.REST(API_KEY, API_SECRET, APCA_API_BASE_URL, 'v2')

    self.stock = 'SPXL'
    self.spxl = 'XIV'
    self.spxs = 'SH'
    self.spxs2 = 'TZA'
    self.timer = 20
    self.timerspxs = 0
    self.timeToClose = 0




  def run(self):
      orders = self.alpaca.list_orders(status="open")
      for order in orders:
          self.alpaca.cancel_order(order.id)

      # Wait for market to open.
      print("Waiting for market to open...")
      tAMO = threading.Thread(target=self.awaitMarketOpen)
      tAMO.start()
      tAMO.join()
      print("Market opened.")

      # Rebalance the portfolio every minute, making necessary trades.
      while True:

          # Figure out when the market will close so we can prepare to sell beforehand.
          clock = self.alpaca.get_clock()
          closingTime = clock.next_close.replace(tzinfo=datetime.timezone.utc).timestamp()
          currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
          self.timeToClose = closingTime - currTime

          if self.timeToClose < (60 * 15):
              current_price = alpaca.get_barset(self.stock, "minute", 1)[self.stock][0].c

              price_history1 = alpaca.get_barset(self.stock, "1D", 20)[self.stock][0].c
              price_history2 = alpaca.get_barset(self.stock, "1D", 50)[self.stock][0].c
              price_history3 = alpaca.get_barset(self.stock, "1D", 200)[self.stock][0].c
              price_history4 = alpaca.get_barset(self.stock, "1D", 3)[self.stock][0].c
              price_history6 = alpaca.get_barset(self.stock, "1D", 9)[self.stock][0].c

              dma20 = price_history1.mean()
              dma50 = price_history2.mean()
              dma200 = price_history3.mean()
              dma5 = price_history4.mean()
              dma9 = price_history6.mean()
              std50 = price_history2.std()
              std200 = price_history3.std()

              account = alpaca.get_account()
              cash = float(account.cash)

              difference1 = (dma20 - dma50)
              difference2 = (dma50 - dma200)
              z1 = difference1 / (std50)
              z2 = difference2 / (std200)
              z6 = (current_price - dma200) / std200
              z7 = (dma5 - dma200) / std200
              #z9 = (dma9 - dma200) / std200

              z2exp = z2exp=z2*z2*z2*1.2-self.timer/60
              #z2exps = z2 * z2 * z2 * 0.9 - timer / 45 - std50 / 20 - 0.1

              #cashper = cash / float(account.portfolio_value)

              if cash / float(account.portfolio_value) > 0.5 or self.timer < 40:
                  self.timer += 1
              if cash / float(account.portfolio_value) > 0.1 and self.timer > 0.5:
                  self.timer -= 0.5
              print("Market closing soon.  Closing positions.")
              positions = self.alpaca.list_positions()
              qty = abs(int(float(positions.qty)))
              respSO = []
              tSubmitOrder = threading.Thread(target=self.submitOrder(qty, positions.symbol, "sell", respSO))
              tSubmitOrder.start()
              tSubmitOrder.join()

              # Run script again after market close for next trading day.
              print("Sleeping until market close (15 minutes).")
              time.sleep(60 * 15)
          else:
              # Rebalance the portfolio.
              tRebalance = threading.Thread(target=self.rebalance)
              tRebalance.start()
              tRebalance.join()
              time.sleep(60)

  # Wait for market to open.
  def awaitMarketOpen(self):
    isOpen = self.alpaca.get_clock().is_open
    while(not isOpen):
      clock = self.alpaca.get_clock()
      openingTime = clock.next_open.replace(tzinfo=datetime.timezone.utc).timestamp()
      currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
      timeToOpen = int((openingTime - currTime) / 60)
      print(str(timeToOpen) + " minutes til market open.")
      time.sleep(60)
      isOpen = self.alpaca.get_clock().is_open

  def rebalance(self):
      #tRerank = threading.Thread(target=self.rerank)
      #tRerank.start()
      #tRerank.join()

      # Clear existing orders again.
      orders = self.alpaca.list_orders(status="open")
      for order in orders:
          self.alpaca.cancel_order(order.id)

      current_price = alpaca.get_barset(self.stock, "minute", 1)[self.stock][0].c

      price_history1 = alpaca.get_barset(self.stock, "1D", 20)[self.stock]
      price_history2 = alpaca.get_barset(self.stock, "1D", 50)[self.stock]
      price_history3 = alpaca.get_barset(self.stock, "1D", 200)[self.stock]
      price_history4 = alpaca.get_barset(self.stock, "1D", 3)[self.stock]
      price_history6 = alpaca.get_barset(self.stock, "1D", 9)[self.stock]

      sum = []
      for i in range(0,20):
          sum.append(price_history1[i].c)
      dma20 = stat.mean(sum)

      sum2 = []
      for i in range(0, 50):
          sum2.append(float(price_history2[i].c))
      dma50 = stat.mean(sum2)
      std50 = stat.stdev(sum2)
      sum3 = []
      for i in range(0,200):
          sum3.append(price_history3[i].c)
      dma200 = stat.mean(sum3)
      std200 = stat.stdev(sum3)
      sum4 = []
      for i in range(0,3):
          sum4.append(price_history4[i].c)
      dma5 = stat.mean(sum4)

      sum5 = []
      for i in range(0,9):
          sum5.append(price_history6[i].c)
      dma9 = stat.mean(sum5)

      #dma20 = price_history1.mean()
      #dma50 = price_history2.mean()
      #dma200 = price_history3.mean()
      #dma5 = price_history4.mean()
      #dma9 = price_history6.mean()
      #std50 = price_history2.std()
      #std200 = price_history3.std()

      account = alpaca.get_account()
      cash = float(account.cash)

      difference1 = (dma20 - dma50)
      difference2 = (dma50 - dma200)
      z1 = difference1 / std50
      z2 = difference2 / std200
      z6 = (current_price - dma200) / std200
      z7 = (dma5 - dma200) / std200
      z9 = (dma9 - dma200) / std200

      z2exp = z2 * z2 * z2 * 1.2 - (self.timer / 60) + std50 / 20
      #z2exps = z2 * z2 * z2 * 0.9 - self.timer / 45 - std50 / 20 - 0.1

      cashper = cash/float(account.portfolio_value)

      # Remove positions that are no longer in the short or long list, and make a list of positions that do not need to change.  Adjust position quantities if needed.
      executed = [[], []]
      positions = self.alpaca.list_positions()
      #self.blacklist.clear()

      if z6 <= z2exp:
          positions = self.alpaca.list_positions()
          qty = abs(int(float(positions.qty)))
          respSO = []
          tSubmitOrder = threading.Thread(target=self.submitOrder(qty, positions.symbol, "sell", respSO))
          tSubmitOrder.start()
          tSubmitOrder.join()

      if z6 > z2exp and z6 < z7 - 0.01:

          if cashper > 0.80:
              respSO = []
              tSO = threading.Thread(target=self.submitOrder,
                                     args=[abs(int(float(0.95*cash/current_price))), self.stock, "buy", respSO])
              tSO.start()
              tSO.join()



      '''
      for position in positions:
          if (self.long.count(position.symbol) == 0):
              # Position is not in long list.
              if (self.short.count(position.symbol) == 0):
                  # Position not in short list either.  Clear position.
                  if (position.side == "long"):
                      side = "sell"
                  else:
                      side = "buy"
                  respSO = []
                  tSO = threading.Thread(target=self.submitOrder,
                                         args=[abs(int(float(position.qty))), position.symbol, side, respSO])
                  tSO.start()
                  tSO.join()
              else:
                  # Position in short list.
                  if (position.side == "long"):
                      # Position changed from long to short.  Clear long position to prepare for short position.
                      side = "sell"
                      respSO = []
                      tSO = threading.Thread(target=self.submitOrder,
                                             args=[int(float(position.qty)), position.symbol, side, respSO])
                      tSO.start()
                      tSO.join()
                  else:
                      if (abs(int(float(position.qty))) == self.qShort):
                          # Position is where we want it.  Pass for now.
                          pass
                      else:
                          # Need to adjust position amount
                          diff = abs(int(float(position.qty))) - self.qShort
                          if (diff > 0):
                              # Too many short positions.  Buy some back to rebalance.
                              side = "buy"
                          else:
                              # Too little short positions.  Sell some more.
                              side = "sell"
                          respSO = []
                          tSO = threading.Thread(target=self.submitOrder,
                                                 args=[abs(diff), position.symbol, side, respSO])
                          tSO.start()
                          tSO.join()
                      executed[1].append(position.symbol)
                      self.blacklist.add(position.symbol)
          else:
              # Position in long list.
              if (position.side == "short"):
                  # Position changed from short to long.  Clear short position to prepare for long position.
                  respSO = []
                  tSO = threading.Thread(target=self.submitOrder,
                                         args=[abs(int(float(position.qty))), position.symbol, "buy", respSO])
                  tSO.start()
                  tSO.join()
              else:
                  if (int(float(position.qty)) == self.qLong):
                      # Position is where we want it.  Pass for now.
                      pass
                  else:
                      # Need to adjust position amount.
                      diff = abs(int(float(position.qty))) - self.qLong
                      if (diff > 0):
                          # Too many long positions.  Sell some to rebalance.
                          side = "sell"
                      else:
                          # Too little long positions.  Buy some more.
                          side = "buy"
                      respSO = []
                      tSO = threading.Thread(target=self.submitOrder, args=[abs(diff), position.symbol, side, respSO])
                      tSO.start()
                      tSO.join()
                  executed[0].append(position.symbol)
                  self.blacklist.add(position.symbol)

      # Send orders to all remaining stocks in the long and short list.
      respSendBOLong = []
      tSendBOLong = threading.Thread(target=self.sendBatchOrder, args=[self.qLong, self.long, "buy", respSendBOLong])
      tSendBOLong.start()
      tSendBOLong.join()
      respSendBOLong[0][0] += executed[0]
      if (len(respSendBOLong[0][1]) > 0):
          # Handle rejected/incomplete orders and determine new quantities to purchase.
          respGetTPLong = []
          tGetTPLong = threading.Thread(target=self.getTotalPrice, args=[respSendBOLong[0][0], respGetTPLong])
          tGetTPLong.start()
          tGetTPLong.join()
          if (respGetTPLong[0] > 0):
              self.adjustedQLong = self.longAmount // respGetTPLong[0]
          else:
              self.adjustedQLong = -1
      else:
          self.adjustedQLong = -1

      respSendBOShort = []
      tSendBOShort = threading.Thread(target=self.sendBatchOrder,
                                      args=[self.qShort, self.short, "sell", respSendBOShort])
      tSendBOShort.start()
      tSendBOShort.join()
      respSendBOShort[0][0] += executed[1]
      if (len(respSendBOShort[0][1]) > 0):
          # Handle rejected/incomplete orders and determine new quantities to purchase.
          respGetTPShort = []
          tGetTPShort = threading.Thread(target=self.getTotalPrice, args=[respSendBOShort[0][0], respGetTPShort])
          tGetTPShort.start()
          tGetTPShort.join()
          if (respGetTPShort[0] > 0):
              self.adjustedQShort = self.shortAmount // respGetTPShort[0]
          else:
              self.adjustedQShort = -1
      else:
          self.adjustedQShort = -1

      # Reorder stocks that didn't throw an error so that the equity quota is reached.
      if (self.adjustedQLong > -1):
          self.qLong = int(self.adjustedQLong - self.qLong)
          for stock in respSendBOLong[0][0]:
              respResendBOLong = []
              tResendBOLong = threading.Thread(target=self.submitOrder,
                                               args=[self.qLong, stock, "buy", respResendBOLong])
              tResendBOLong.start()
              tResendBOLong.join()

      if (self.adjustedQShort > -1):
          self.qShort = int(self.adjustedQShort - self.qShort)
          for stock in respSendBOShort[0][0]:
              respResendBOShort = []
              tResendBOShort = threading.Thread(target=self.submitOrder,
                                                args=[self.qShort, stock, "sell", respResendBOShort])
              tResendBOShort.start()
              tResendBOShort.join()

  def sendBatchOrder(self, qty, stocks, side, resp):
      executed = []
      incomplete = []
      for stock in stocks:
          if (self.blacklist.isdisjoint({stock})):
              respSO = []
              tSubmitOrder = threading.Thread(target=self.submitOrder, args=[qty, stock, side, respSO])
              tSubmitOrder.start()
              tSubmitOrder.join()
              if (not respSO[0]):
                  # Stock order did not go through, add it to incomplete.
                  incomplete.append(stock)
              else:
                  executed.append(stock)
              respSO.clear()
      resp.append([executed, incomplete])

      # Submit an order if quantity is above 0.
'''
  def submitOrder(self, qty, stock, side, resp):
      if (qty > 0):
          try:
              self.alpaca.submit_order(stock, qty, side, "market", "day")
              print("Market order of | " + str(qty) + " " + stock + " " + side + " | completed.")
              resp.append(True)
          except:
              print("Order of | " + str(qty) + " " + stock + " " + side + " | did not go through.")
              resp.append(False)
      else:
          print("Quantity is 0, order of | " + str(qty) + " " + stock + " " + side + " | not completed.")
          resp.append(True)

      # Get percent changes of the stock prices over the past 10 days.

  def getPercentChanges(self):
      length = 10
      for i, stock in enumerate(self.allStocks):
          bars = self.alpaca.get_barset(stock[0], 'minute', length)
          self.allStocks[i][1] = (bars[stock[0]][len(bars[stock[0]]) - 1].c - bars[stock[0]][0].o) / bars[stock[0]][0].o

      # Mechanism used to rank the stocks, the basis of the Long-Short Equity Strategy.

  def rank(self):
      # Ranks all stocks by percent change over the past 10 days (higher is better).
      tGetPC = threading.Thread(target=self.getPercentChanges)
      tGetPC.start()
      tGetPC.join()

      # Sort the stocks in place by the percent change field (marked by pc).
      self.allStocks.sort(key=lambda x: x[1])


# Run the LongShort class
ls = LongShort()
ls.run()
