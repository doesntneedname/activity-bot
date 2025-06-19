const axios = require('axios');
const cron = require('node-cron');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

const METABASE_URL = process.env.METABASE_URL;
const BEARER_TOKEN = process.env.BEARER_TOKEN;
const BASIC_AUTH_USER = process.env.BASIC_AUTH_USER;
const BASIC_AUTH_PASS = process.env.BASIC_AUTH_PASS;
const USERNAME = process.env.USERNAME; 
const PASSWORD = process.env.PASSWORD; 
const POST_URL = 'https://api.pachca.com/api/shared/v1/messages';

const QUESTION_IDS = [601, 602, 613, 612, 614, 604, 610, 611, 603, 605]; 

let cachedData = null; 

// Получение токена сессии
async function getSessionToken() {
    const response = await axios.post(
        `${METABASE_URL}/api/session`,
        { username: USERNAME, password: PASSWORD },
        {
            headers: { 'Content-Type': 'application/json' },
            auth: { username: BASIC_AUTH_USER, password: BASIC_AUTH_PASS },
        }
    );
    console.log('Session Token получен');
    return response.data.id;
}

// Функция для форматирования чисел
function formatNumber(num) {
    return num.toLocaleString('en-US');
}

// Функция для чтения и обновления кеша
function readCache() {
    try {
        if (!fs.existsSync('cache.json')) {
            console.log('Файл cache.json отсутствует. Создаем новый файл.');
            fs.writeFileSync('cache.json', JSON.stringify({}, null, 2), 'utf8');
        }
        const content = fs.readFileSync('cache.json', 'utf8');
        if (!content.trim()) {
            console.warn('Файл cache.json пуст. Инициализируем пустым объектом.');
            fs.writeFileSync('cache.json', JSON.stringify({}, null, 2), 'utf8');
            return {};
        }
        return JSON.parse(content);
    } catch (error) {
        console.error('Ошибка при чтении cache.json:', error.message);
        fs.writeFileSync('cache.json', JSON.stringify({}, null, 2), 'utf8');
        return {};
    }
}

function updateCache(questionId, value) {
    try {
        const cache = readCache();
        cache[questionId] = value;
        fs.writeFileSync('cache.json', JSON.stringify(cache, null, 2));
        console.log(`Кеш обновлен для questionId ${questionId}: ${value}`);
    } catch (error) {
        console.error('Ошибка при обновлении кеша:', error.message);
    }
}

function getCachedValue(questionId) {
    const cache = readCache();
    return cache[questionId] || 0;
}

// Функция для получения данных вопроса
async function fetchQuestionData(questionId, sessionToken) {
    try {
        const response = await axios.post(
            `${METABASE_URL}/api/card/${questionId}/query`,
            {},
            {
                headers: {
                    'Content-Type': 'application/json',
                    'X-Metabase-Session': sessionToken,
                },
                auth: { username: BASIC_AUTH_USER, password: BASIC_AUTH_PASS },
            }
        );

        const rows = response.data.data?.rows || [];
        const columns = response.data.data?.results_metadata?.columns || [];

        if (!rows.length || !columns.length) {
            console.error(`Данные вопроса ${questionId} пусты или имеют некорректную структуру.`);
            return [];
        }

        return rows.map(row =>
            row.reduce((acc, value, index) => {
                acc[columns[index].display_name || columns[index].name] = value;
                return acc;
            }, {})
        );
    } catch (error) {
        console.error(`Ошибка при получении данных вопроса ${questionId}:`, error.message);
        return [];
    }
}

// Функция для формирования первого payload
function createPrimaryPayload(allData) {
    const summaryData = allData.find(item => item.questionId === 611)?.data || [];
    const totalSegment = summaryData.find(segment => segment.segment === 'Total') || {};

    const currData = allData.find(item => item.questionId === 610)?.data || [];
    const currFormatted = currData.map(segment => formatNumber(segment.curr_percentage)).join('% / ');

    const primaryPayload = {
        message: {
            entity_type: "discussion",
            entity_id: 24592837,
            content: `DAU / WAU / MAU: ${formatNumber(totalSegment.dau)} / ${formatNumber(totalSegment.wau)} / ${formatNumber(totalSegment.mau)}\n` +
                     `New / Current / Dormant: ${formatNumber(totalSegment.new_users)} / ${formatNumber(totalSegment.current_users)} / ${formatNumber(totalSegment.dormant_users)}\n\n` +
                     `CURR / Ed / Pay / Free: ${currFormatted}%`,
        },
    };
    console.log('Первичный Payload:', JSON.stringify(primaryPayload, null, 2));
    return primaryPayload;
}

// Функция отправки первичного payload (из оригинального кода)
async function sendPrimaryPayload(payload, allData) {
    try {
        const response = await axios.post(POST_URL, payload, {
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${BEARER_TOKEN}`,
            },
        });
        console.log('Первичный Payload успешно отправлен:', response.data);

        // Извлечение id из ответа
        const messageId = response.data.data.id;
        if (messageId) {
            await createThreadForMessage(messageId, allData);
        }
    } catch (error) {
        console.error('Ошибка при отправке первичного Payload:', error.message);
    }
}

// Функция для создания треда (из оригинального кода)
async function createThreadForMessage(messageId, allData) {
    try {
        const threadResponse = await axios.post(
            `${POST_URL}/${messageId}/thread`,
            {},
            {
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${BEARER_TOKEN}`,
                },
            }
        );

        console.log('Тред успешно создан:', threadResponse.data);

        // Отправка вторичного сообщения в тред
        const threadId = threadResponse.data.data.id;
        if (threadId) {
            await sendSecondaryPayload(threadId, allData);
        }
    } catch (error) {
        console.error('Ошибка при создании треда:', error.message);
    }
}

// Функция для отправки вторичного payload (из оригинального кода)
async function sendSecondaryPayload(threadId, allData) {
    try {
        const counts = allData.filter(item => [601, 602, 603, 604, 605].includes(item.questionId))
            .map(item => {
                const metric = item.data[0] || {};
                switch (item.questionId) {
                    case 602: return `Компании: ${formatNumber(metric.Count || 0)}`;
                    case 603: return `Лички: ${formatNumber(metric.Count || 0)}`;
                    case 604: return `Беседы/каналы: ${formatNumber(metric.Count || 0)}`;
                    case 605: return `Треды: ${formatNumber(metric.Count || 0)}`;
                    default: return "";
                }
            }).join('\n');

        const messageTotals = allData.filter(item => [613, 612, 614].includes(item.questionId))
            .map(item => {
                const metric = item.data[0] || {};
                const previousValue = getCachedValue(item.questionId);
                const currentValue = metric["Sum of Messages Count"] || 0;
                const difference = currentValue - previousValue;
                updateCache(item.questionId, currentValue);

                switch (item.questionId) {
                    case 613: return `Беседы/каналы: ${formatNumber(difference)}`;
                    case 612: return `Лички: ${formatNumber(difference)}`;
                    case 614: return `Треды: ${formatNumber(difference)}`;
                    default: return "";
                }
            }).join('\n');

        const formattedContent = `👨‍💻Daily Activ:{${counts}\n\n` +
                                 `💬Daily Messages:\n${messageTotals}`;

        const secondaryPayload = {
            message: {
                entity_type: "thread",
                entity_id: threadId,
                content: formattedContent,
            },
        };

        const response = await axios.post(POST_URL, secondaryPayload, {
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${BEARER_TOKEN}`,
            },
        });

        console.log('Вторичный Payload успешно отправлен:', response.data);
    } catch (error) {
        console.error('Ошибка при отправке вторичного Payload:', error.message);
    }
}

// Основная функция обработки (только сбор данных в 23:50 по UTC)
async function processQuestions() {
    try {
        const sessionToken = await getSessionToken();
        const allData = [];

        for (const questionId of QUESTION_IDS) {
            const data = await fetchQuestionData(questionId, sessionToken);
            if (data.length > 0) {
                allData.push({ questionId, data });
            } else {
                console.warn(`Данные для вопроса ${questionId} не были отправлены, так как они пусты.`);
            }
        }

        // Сохраняем собранные данные в переменную
        cachedData = allData;
        console.log('Данные собраны и сохранены для последующей отправки.');
    } catch (error) {
        console.error('Общая ошибка при сборе данных:', error.message);
    }
}

// Функция для отправки сохраненных данных (в 9:00 по UTC+3)
async function sendCachedData() {
    try {
        if (!cachedData) {
            console.warn('Нет сохраненных данных для отправки.');
            return;
        }
        // Используем ранее сохраненные данные
        const allData = cachedData;
        const primaryPayload = createPrimaryPayload(allData);
        await sendPrimaryPayload(primaryPayload, allData);
        console.log('Сохраненные данные успешно отправлены.');
    } catch (error) {
        console.error('Ошибка при отправке сохраненных данных:', error.message);
    }
}
// Запуск задачи в 23:50 по UTC для сбора данных
cron.schedule('50 23 * * *', () => {
    console.log('Запуск задачи для сбора данных в 23:50 по UTC');
    processQuestions();
}, {
    timezone: 'Etc/UTC'
});

// Запуск задачи в 9:00 по UTC+3 для отправки отчета
cron.schedule('0 9 * * *', () => {
    console.log('Отправка отчета в 9:00 по UTC+3');
    sendCachedData();
}, {
    timezone: 'Europe/Moscow'
});

console.log('Запуск...');
