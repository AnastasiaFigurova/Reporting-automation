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
SELECT toStartOfDay(toDateTime(time)) as dat,
COUNT(DISTINCT user_id) AS dau,
countIf(user_id, action = 'like') AS likes,
countIf(user_id, action = 'view') AS views,
countIf(user_id, action = 'like') / countIf(user_id, action = 'view') AS CTR
FROM simulator_20220420.feed_actions
WHERE dat BETWEEN today()-7 AND today()-1
GROUP BY dat
ORDER BY dat 
"""
df = pandahouse.read_clickhouse(q, connection=connection)

#calculation of yesterday metrics and sending metrics
date = str(df.dat.iloc[-1]).split()[0]
DAU = df.dau.iloc[-1]
views = df.views.iloc[-1]
likes = df.likes.iloc[-1]
CTR = round(df.CTR.iloc[-1],3)
mes = f' Отчёт за предыдущий день: {date} \n DAU: {DAU} \n Количество просмотров: {views} \n Количество лайков: {likes} \n CTR: {CTR}'
bot.sendMessage(chat_id=chat_id, text = mes)

#Making plot and sending plot
fig, axes = plt.subplots(nrows = 3, ncols = 1, figsize = (7, 16))
axes[0].plot(df["dat"], df["dau"], label = "Уникальные пользователи", color = "tomato")
axes[0].set_title("DAU за последние 7 дней", fontsize = 13, pad = 15)
axes[0].tick_params(axis='x', labelrotation=45)
axes[0].legend(fontsize = 11)
axes[0].grid()

axes[1].plot(df["dat"], df["views"], label = "Просмотры", color = "darkorange")
axes[1].plot(df["dat"], df["likes"], label = "Лайки", color = "teal")
axes[1].set_title('Количество просмотров и лайков за последние 7 дней', fontsize = 13, pad = 15)
axes[1].tick_params(axis='x', labelrotation=45)
axes[1].legend(fontsize = 11)
axes[1].grid()

axes[2].plot(df["dat"], df["CTR"], label = "CTR", color = "darkred")
axes[2].set_title('CTR за последние 7 дней', fontsize = 13, pad = 15)
axes[2].tick_params(axis='x', labelrotation=45)
axes[2].legend(fontsize = 11)
axes[2].grid()
plt.subplots_adjust(wspace = 0.5, hspace = 0.5)
plot_object = io.BytesIO()
fig.savefig(plot_object)
plot_object.seek(0)
plot_object.name = 'report_plot.png'
plt.close()
bot.sendPhoto(chat_id=chat_id, photo=plot_object)

plt.show()
