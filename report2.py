import telegram
import pandahouse
import matplotlib.pyplot as plt
import io

#create bot
bot = telegram.Bot(token = "5373576465:AAFHbGFKlmy3bpRzH4vxZtCUZ-jc_KHWo5M")
chat_id = -799682816

#data loading
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator'
}
q = """
SELECT  dat, countIf(user_id, feeds > 0 and messages = 0) as onlyfeed,
countIf(user_id, feeds = 0 and messages > 0) as onlymsg,
countIf(user_id, feeds > 0 and messages > 0) as feedmsg,
sum(views) as views, sum(likes) as likes
 FROM (SELECT user_id,
             toStartOfDay(toDateTime(time)) as dat,
             COUNT(post_id) as feeds,
             countIf(user_id, action='view') as views,
             countIf(user_id, action='like') as likes
      FROM simulator_20220420.feed_actions
      GROUP BY user_id,
               dat
      HAVING dat BETWEEN today()-7 AND today()-1) as t1
   FULL JOIN
     (SELECT user_id,
             toStartOfDay(toDateTime(time)) as dat,
             COUNT(reciever_id) as messages
      FROM simulator_20220420.message_actions
      GROUP BY user_id,
               dat
      HAVING dat BETWEEN today()-7 AND today()-1) AS t2 USING user_id, dat
GROUP BY dat
ORDER BY dat 
"""
df = pandahouse.read_clickhouse(q, connection=connection)

#calculation of yesterday metrics and sending metrics
data = str(df.dat.iloc[-1]).split()[0]
of = df.onlyfeed.iloc[-1]
om = df.onlymsg.iloc[-1]
fm = df.feedmsg.iloc[-1]
view = df.views.iloc[-1]
like = df.likes.iloc[-1]
mes = f' Отчёт по работе всего приложения за предыдущий день: {data}\n Пользователи только ленты: {of} \n Пользователи только мессенджера: {om} \n Пользователи ленты и мессенджера: {fm} \n Количество просмотров: {view} \n Количество лайков: {like}'
bot.sendMessage(chat_id=chat_id, text = mes)

#Making plot and sending plot
fig, axes = plt.subplots(nrows = 1, ncols = 2, figsize = (17, 5))
axes[0].plot(df["dat"], df["onlyfeed"], label = "Лента", color = "blue")
axes[0].plot(df["dat"], df["onlymsg"], label = "Сообщения", color = "green")
axes[0].plot(df["dat"], df["feedmsg"], label = "Лента + сообщения", color = "tomato")
axes[0].set_title("Основная активность пользователей за последние 7 дней", fontsize = 12, pad = 15)
axes[0].tick_params(axis = "x", labelrotation = 45)
axes[0].legend(fontsize = 11)
axes[0].grid()

axes[1].plot(df["dat"], df["views"], label = "Просмотры", color = "mediumblue")
axes[1].plot(df["dat"], df["likes"], label = "Лайки", color = "purple")
axes[1].plot(df["dat"], df["onlymsg"], label = "Сообщения", color = "green")
axes[1].set_title("Основная активность пользователей по типам активности за последние 7 дней", fontsize = 12, pad = 15)
axes[1].tick_params(axis = "x", labelrotation = 45)
axes[1].legend(fontsize = 11)
axes[1].grid()

plt.subplots_adjust(wspace = 0.2)
plot_object = io.BytesIO()
fig.savefig(plot_object)
plot_object.seek(0)
plot_object.name = 'report_plot.png'
plt.close()
bot.sendPhoto(chat_id=chat_id, photo=plot_object)

plt.show()

#data loading and sending data frame
a = """
SELECT post_id AS post_id,
       countIf(action = 'view') AS "Просмотры",
       countIf(action = 'like') AS "Лайки",
       countIf(action = 'like') / countIf(action = 'view') AS "CTR",
       count(DISTINCT user_id) AS "Охват"
FROM simulator_20220420.feed_actions
GROUP BY post_id
ORDER BY "Просмотры" DESC
LIMIT 20
"""
data = pandahouse.read_clickhouse(a, connection=connection)
file_object = io.StringIO()
data.to_csv(file_object)
file_object.seek(0)
file_object.name = 'top_20_posts.csv'

bot.sendDocument(chat_id = chat_id, document = file_object)

#data loading and sending data frame
b = """
SELECT user_id AS user_id,
       sum(views) AS "Просмотры",
       sum(likes) AS "Лайки",
       sum(messages) AS "Сообщения"
FROM
  (SELECT *
   FROM
     (SELECT user_id,
             toDate(time) as "date",
             COUNT(post_id) as feeds,
             countIf(user_id, action='view') as views,
             countIf(user_id, action='like') as likes
      FROM simulator_20220420.feed_actions
      GROUP BY user_id,
               "date") as t1
   FULL JOIN
     (SELECT user_id,
             toDate(time) as "date",
             COUNT(reciever_id) as messages
      FROM simulator_20220420.message_actions
      GROUP BY user_id,
               "date") AS t2 USING user_id,
                                   "date") AS virtual_table
GROUP BY user_id
ORDER BY "Просмотры" DESC
LIMIT 20
"""
frame = pandahouse.read_clickhouse(b, connection=connection)
file_object = io.StringIO()
frame.to_csv(file_object)
file_object.seek(0)
file_object.name = 'top_20_users.csv'

bot.sendDocument(chat_id = chat_id, document = file_object)