# 🤖 TG-Bot-LZT

Telegram-бот для работы с API **Lolz.live / Zelenka (LZT)**: переводы, выводы, инвойсы, автоподнятие тем, уведомления.

## 📌 Возможности
- 💼 Баланс + история операций (Market API)
- 💸 Переводы пользователям (ID, @username, ссылка) + поддержка холда
- 🧾 Создание инвойсов на оплату
- 🏦 Вывод средств (выбор сервиса, кошелёк, include_fee, extra)
- 🔔 Push-уведомления: лайки, ответы, упоминания, посты, пополнения, снятие холда
- ⏫ Автоподнятие тем по расписанию (каждые N минут)
- 🗒 Секретные заметки (привязка к переводам и инвойсам)
- Меню и FSM формы на **aiogram 3.x**

---

## ⚙️ Установка
```bash
git clone https://github.com/LucyLZT/tg-bot-lzt.git
cd tg-bot-lzt

python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/macOS:
source .venv/bin/activate

pip install -r requirements.txt
