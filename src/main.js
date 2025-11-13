// Rekrute.com jobs scraper - production-ready CheerioCrawler implementation
// Stack: Apify SDK + Crawlee + CheerioCrawler + header-generator + jsdom (optional fallback)

import { Actor, log } from 'apify';
import {
    CheerioCrawler,
    Dataset,
    sleep,
    RequestQueue,
} from 'crawlee';
import { load as cheerioLoad } from 'cheerio';
import { HeaderGenerator } from 'header-generator';
import { JSDOM } from 'jsdom';

// ---------- Global config & helpers ----------

// Reuse a single header generator for stealthy browser-like headers.
const headerGenerator = new HeaderGenerator({
    browsers: ['chrome'],
    devices: ['desktop'],
    operatingSystems: ['windows', 'linux'],
    locales: ['fr-FR', 'fr', 'en-US', 'en'],
});

/**
 * Normalize and clamp numeric input.
 */
function toPositiveInt(value, defaultValue, { min = 1, max = 100000 } = {}) {
    const n = Number(value);
    if (!Number.isFinite(n)) return defaultValue;
    return Math.min(max, Math.max(min, Math.floor(n)));
}

/**
 * Return absolute URL given a possibly relative href and base.
 */
function toAbs(href, base) {
    try {
        return new URL(href, base).href;
    } catch {
        return null;
    }
}

/**
 * Basic "blocked/captcha" heuristic.
 */
function isBlocked($) {
    const text = $('body').text().toLowerCase();
    return /captcha|access denied|forbidden|blocked|unusual traffic/.test(text);
}

/**
 * Clean HTML to a normalized text string.
 */
function cleanText(htmlOrText) {
    if (!htmlOrText) return null;

    // If it's HTML, strip tags via cheerio
    const $ = cheerioLoad(`<div id="root">${htmlOrText}</div>`);
    let text = $('#root').text();

    if (!text) return null;

    text = text
        .replace(/\r\n|\r/g, '\n')
        .replace(/\u00a0/g, ' ')
        .replace(/\s+\n/g, '\n')
        .replace(/\n\s+/g, '\n')
        .replace(/[ \t]{2,}/g, ' ')
        .trim();

    return text || null;
}

/**
 * Try to extract JobPosting data from JSON-LD.
 * Returns a plain object with a few normalized fields or null.
 */
function extractFromJsonLd($) {
    const scripts = $('script[type="application/ld+json"]');

    for (let i = 0; i < scripts.length; i++) {
        try {
            const jsonText = $(scripts[i]).html() || '';
            if (!jsonText.trim()) continue;

            const parsed = JSON.parse(jsonText);

            const nodes = [];
            const addNode = (node) => {
                if (!node) return;
                if (Array.isArray(node)) {
                    node.forEach(addNode);
                } else if (node['@graph']) {
                    addNode(node['@graph']);
                } else {
                    nodes.push(node);
                }
            };
            addNode(parsed);

            for (const e of nodes) {
                const t = e['@type'] || e.type;
                const types = Array.isArray(t) ? t : [t];

                if (types.includes('JobPosting')) {
                    const jobLoc = e.jobLocation || {};
                    const addr = jobLoc.address || {};

                    return {
                        raw: e,
                        title: e.title || e.name || null,
                        company: e.hiringOrganization?.name || null,
                        datePosted: e.datePosted || null,
                        descriptionHtml: e.description || null,
                        employmentType: e.employmentType || null,
                        validThrough: e.validThrough || null,
                        location:
                            addr.addressLocality ||
                            addr.addressRegion ||
                            addr.addressCountry ||
                            null,
                        salary: e.baseSalary?.value?.value
                            ? `${e.baseSalary.value.value} ${e.baseSalary.value.currency || ''}`.trim()
                            : null,
                    };
                }
            }
        } catch {
            // ignore JSON errors and continue
        }
    }

    return null;
}

/**
 * Parse title, company, location from heading + DOM.
 * Multi-language friendly and defensive.
 */
function parseTitleCompanyLocation($) {
    // Primary: <h1>Title - Company - Location</h1> (or variations)
    const h1 = $('h1').first().text().trim();
    let title = null;
    let company = null;
    let location = null;

    if (h1) {
        const parts = h1.split(/\s[-–|]\s/); // split on -, – or |
        if (parts.length >= 1) title = parts[0]?.trim() || null;
        if (parts.length >= 2) company = parts[1]?.trim() || null;
        if (parts.length >= 3) location = parts[2]?.trim() || null;
    }

    // Fallback: dedicated company element
    if (!company) {
        const companySel =
            '.company, .company-name, .societe, .society, a.company, a.company-name';
        company = $(companySel).first().text().trim() || null;
    }

    // Fallback: location label in FR/EN
    if (!location) {
        const locLabel = $('p:contains("Poste basé à"), p:contains("Localisation"), p:contains("Location")')
            .first()
            .text();
        if (locLabel) {
            const m = locLabel.match(/(?:basé à|Localisation|Location)\s*[:\-]?\s*(.*)/i);
            if (m && m[1]) location = m[1].trim();
        }
    }

    return { title, company, location };
}

/**
 * Extract description HTML from FR and EN section headings,
 * with a broad fallback to main job description container.
 */
function getDescriptionHtml($) {
    let html = '';

    // FR sections
    const posteFr = $('h2:contains("Poste :")').nextUntil('h2').html();
    const profilFr = $('h2:contains("Profil recherché :")').nextUntil('h2').html();

    // EN sections (approximate)
    const posteEn = $('h2:contains("Job Description"), h2:contains("Position"), h2:contains("Role")')
        .nextUntil('h2')
        .html();
    const profilEn = $('h2:contains("Profile"), h2:contains("Requirements"), h2:contains("Candidate profile")')
        .nextUntil('h2')
        .html();

    if (posteFr) html += posteFr;
    if (profilFr) html += profilFr;
    if (posteEn) html += posteEn;
    if (profilEn) html += profilEn;

    // Broad fallback: main job container (guessing common patterns)
    if (!html) {
        const container = $(
            '#job_desc, #job, #job-info, .job-description, .annonce, .job, .jobDetail'
        ).first();
        if (container.length) {
            html = container.html() || '';
        }
    }

    if (!html) return null;

    // Optional: run through jsdom for mild normalization
    try {
        const dom = new JSDOM(`<div id="root">${html}</div>`);
        const root = dom.window.document.getElementById('root');
        return root.innerHTML || html;
    } catch {
        return html;
    }
}

/**
 * Extract posted date from textual hints in FR + EN.
 */
function getDatePosted($) {
    // FR: "Publiée le ..."
    const frText = $('p:contains("Publiée")').first().text();
    if (frText) {
        const m = frText.match(/Publiée\s+(?:le\s+)?(.*)/i);
        if (m && m[1]) return m[1].trim();
    }

    // EN: "Published on ...", "Posted on ..."
    const enText = $('p:contains("Published"), p:contains("Posted")').first().text();
    if (enText) {
        const m = enText.match(/(?:Published|Posted)\s+(?:on\s+)?(.*)/i);
        if (m && m[1]) return m[1].trim();
    }

    return null;
}

/**
 * Find job detail links on a listing page (FR + EN).
 * Uses domain + path heuristics, then refined regex.
 */
function findJobLinks($, baseUrl) {
    const links = new Set();

    $('a[href]').each((_, el) => {
        const href = $(el).attr('href');
        if (!href) return;

        const abs = toAbs(href, baseUrl);
        if (!abs) return;

        let url;
        try {
            url = new URL(abs);
        } catch {
            return;
        }

        // Host guard: must stay on rekrute.com
        if (!/\.?rekrute\.com$/i.test(url.hostname)) return;

        const path = url.pathname || '';

        // Heuristics for job offer URLs:
        // - French often includes "offre-emploi"
        // - English may keep same or use a similar slug
        if (
            /\/offre-emploi/i.test(path) ||
            /\/job-offer/i.test(path) ||
            /-emploi-/i.test(path)
        ) {
            links.add(url.href);
        }
    });

    return [...links];
}

/**
 * Find next page URL from pagination.
 * Uses rel="next" and FR+EN "Next" labels.
 */
function findNextPage($, baseUrl) {
    // 1) rel="next"
    const relNextHref =
        $('.pagination a[rel="next"]').attr('href') ||
        $('a[rel="next"]').attr('href');

    if (relNextHref) {
        const abs = toAbs(relNextHref, baseUrl);
        if (abs) return abs;
    }

    // 2) Text-based FR + EN
    const nextLink = $('a')
        .filter((_, el) => {
            const txt = $(el).text().trim().toLowerCase();
            return /(suivant|next|›|»|>)/.test(txt);
        })
        .first()
        .attr('href');

    if (nextLink) {
        const abs = toAbs(nextLink, baseUrl);
        if (abs) return abs;
    }

    return null;
}

/**
 * Build listing URL based on keyword/location/category and language.
 * Uses `clear=1` as requested.
 */
function buildStartUrl({ keyword, location, category, lang }) {
    let base;
    if (lang === 'en') {
        base = 'https://www.rekrute.com/en/offres.html';
    } else {
        // default FR
        base = 'https://www.rekrute.com/offres.html';
    }

    const u = new URL(base);
    u.searchParams.set('clear', '1');

    if (keyword) u.searchParams.set('keyword', String(keyword).trim());
    if (location) u.searchParams.set('location', String(location).trim());
    if (category) u.searchParams.set('category', String(category).trim());

    return u.href;
}

// ---------- Main actor ----------

await Actor.init(); // for environments that don't use Actor.main

async function main() {
    try {
        const input = (await Actor.getInput()) || {};
        const {
            keyword = '',
            location = '',
            category = '',
            results_wanted: RESULTS_WANTED_RAW = 100,
            max_pages: MAX_PAGES_RAW = 999,
            collectDetails = true,
            startUrl,
            startUrls,
            url,
            proxyConfiguration,
            lang = 'fr', // 'fr' | 'en' | 'both'
        } = input;

        const RESULTS_WANTED = toPositiveInt(RESULTS_WANTED_RAW, 100, { min: 1, max: 10000 });
        const MAX_PAGES = toPositiveInt(MAX_PAGES_RAW, 999, { min: 1, max: 10000 });

        log.info('Input received', {
            keyword,
            location,
            category,
            RESULTS_WANTED,
            MAX_PAGES,
            collectDetails,
            lang,
        });

        // Build initial URLs
        const initial = [];

        if (Array.isArray(startUrls) && startUrls.length) {
            for (const u of startUrls) {
                if (u) initial.push(String(u));
            }
        }
        if (startUrl) initial.push(String(startUrl));
        if (url) initial.push(String(url));

        if (!initial.length) {
            if (lang === 'both') {
                initial.push(buildStartUrl({ keyword, location, category, lang: 'fr' }));
                initial.push(buildStartUrl({ keyword, location, category, lang: 'en' }));
            } else {
                initial.push(buildStartUrl({ keyword, location, category, lang }));
            }
        }

        log.info(`Initial start URLs:`, initial);

        const proxyConf = proxyConfiguration
            ? await Actor.createProxyConfiguration(proxyConfiguration)
            : null;

        const requestQueue = await RequestQueue.open();

        // Seed queue
        for (const start of initial) {
            await requestQueue.addRequest({
                url: start,
                userData: {
                    label: 'LIST',
                    pageNo: 1,
                },
            });
        }

        // Global state across handlers
        const visitedListUrls = new Set();
        const seenJobUrls = new Set();

        let saved = 0;
        let queuedDetail = 0;
        let listPages = 0;
        let detailPages = 0;

        const crawler = new CheerioCrawler({
            requestQueue,
            proxyConfiguration: proxyConf || undefined,
            maxConcurrency: 5,
            requestHandlerTimeoutSecs: 120,
            navigationTimeoutSecs: 60,
            maxRequestRetries: 3,
            useSessionPool: true,
            sessionPoolOptions: {
                maxPoolSize: 50,
                sessionOptions: {
                    maxUsageCount: 50,
                },
            },
            preNavigationHooks: [
                async (crawlingContext) => {
                    const { request, session } = crawlingContext;

                    // Generate browser-like headers
                    const generated = headerGenerator.getHeaders({
                        httpVersion: '2',
                    });

                    request.headers = {
                        ...generated,
                        ...request.headers,
                    };

                    // Light randomized delay to look more human
                    await sleep(1000 + Math.random() * 2000);

                    log.debug('Requesting URL', {
                        url: request.url,
                        sessionId: session?.id,
                    });
                },
            ],
            failedRequestHandler: async ({ request, error, session }) => {
                log.error(`Request failed ${request.url}: ${error?.message || error}`);
                if (session) session.retire();
            },
            requestHandler: async (ctx) => {
                const {
                    request,
                    $,
                    body,
                    contentType,
                    session,
                } = ctx;

                const label = request.userData.label || 'LIST';
                const pageNo = request.userData.pageNo || 1;

                // Ensure we have a cheerio instance
                let $page = $;
                if (!$page) {
                    const html = typeof body === 'string' ? body : body?.toString('utf8');
                    if (!html) {
                        log.warning(`Empty body for ${label} ${request.url}`);
                        return;
                    }
                    $page = cheerioLoad(html);
                }

                // Detect block / captcha
                if (isBlocked($page)) {
                    log.warning(`Potentially blocked page at ${request.url}`);
                    if (session) session.retire();
                    return;
                }

                if (label === 'LIST') {
                    listPages++;
                    const listUrl = request.loadedUrl || request.url;

                    if (visitedListUrls.has(listUrl)) {
                        log.debug(`Already visited LIST URL, skipping: ${listUrl}`);
                        return;
                    }
                    visitedListUrls.add(listUrl);

                    const anchorCount = $page('a[href]').length;
                    log.info(`LIST page ${pageNo} (${listUrl}) has ${anchorCount} links.`);

                    const remaining = RESULTS_WANTED - saved - queuedDetail;
                    if (remaining <= 0) {
                        log.info(`Already queued/saved ${RESULTS_WANTED} jobs, skipping new links on LIST.`);
                        return;
                    }

                    // Find job links
                    const jobLinks = findJobLinks($page, listUrl);
                    log.info(`Found ${jobLinks.length} job links on LIST page ${pageNo}.`);

                    if (!jobLinks.length) {
                        const snippet = $page('body').text().trim().slice(0, 200);
                        log.warning(
                            `No job links found on LIST page ${pageNo}: ${listUrl}. Body snippet: "${snippet}..."`
                        );
                    }

                    const newJobLinks = [];
                    for (const link of jobLinks) {
                        if (seenJobUrls.has(link)) continue;
                        seenJobUrls.add(link);
                        newJobLinks.push(link);
                    }

                    if (collectDetails) {
                        const allowed = newJobLinks.slice(0, remaining);
                        for (const jobUrl of allowed) {
                            await requestQueue.addRequest({
                                url: jobUrl,
                                userData: { label: 'DETAIL' },
                            });
                            queuedDetail++;
                        }
                        log.info(
                            `Queued ${allowed.length} new DETAIL requests (queuedDetail=${queuedDetail}, saved=${saved}).`
                        );
                    } else {
                        // URL-only mode: store URLs directly from list pages
                        const allowed = newJobLinks.slice(0, remaining);
                        for (const jobUrl of allowed) {
                            await Dataset.pushData({
                                url: jobUrl,
                                source: 'rekrute.com',
                                discoveredOn: listUrl,
                                pageNo,
                                scrapedAt: new Date().toISOString(),
                            });
                            saved++;
                        }

                        log.info(
                            `Saved ${allowed.length} URL-only items (saved=${saved}/${RESULTS_WANTED}).`
                        );
                    }

                    // Pagination
                    if (pageNo < MAX_PAGES && saved < RESULTS_WANTED) {
                        const nextPageUrl = findNextPage($page, listUrl);

                        if (nextPageUrl && !visitedListUrls.has(nextPageUrl)) {
                            log.info(`Enqueuing next LIST page: ${nextPageUrl}`);
                            await requestQueue.addRequest({
                                url: nextPageUrl,
                                userData: {
                                    label: 'LIST',
                                    pageNo: pageNo + 1,
                                },
                            });
                        } else if (nextPageUrl) {
                            log.debug(
                                `Next LIST page already visited or queued: ${nextPageUrl}`
                            );
                        } else {
                            log.info(
                                `No next LIST page found from ${listUrl} (pageNo=${pageNo})`
                            );
                        }
                    } else if (pageNo >= MAX_PAGES) {
                        log.info(
                            `Reached MAX_PAGES=${MAX_PAGES}, not following pagination from ${listUrl}.`
                        );
                    }
                } else if (label === 'DETAIL') {
                    detailPages++;

                    if (!collectDetails) {
                        log.debug(
                            `DETAIL reached but collectDetails=false, skipping: ${request.url}`
                        );
                        return;
                    }

                    if (saved >= RESULTS_WANTED) {
                        log.info(
                            `Saved >= RESULTS_WANTED (${RESULTS_WANTED}), skipping DETAIL: ${request.url}`
                        );
                        return;
                    }

                    const detailUrl = request.loadedUrl || request.url;

                    try {
                        const pageLang =
                            $page('html').attr('lang') ||
                            $page('html').attr('xml:lang') ||
                            null;

                        const jsonLd = extractFromJsonLd($page);
                        const { title: titleHtml, company: companyHtml, location: locationHtml } =
                            parseTitleCompanyLocation($page);
                        const descriptionHtml = getDescriptionHtml($page);

                        const descriptionText =
                            cleanText(jsonLd?.descriptionHtml) ||
                            cleanText(descriptionHtml);

                        const datePostedLd = jsonLd?.datePosted || null;
                        const datePostedText = getDatePosted($page);
                        const datePosted = datePostedLd || datePostedText || null;

                        const finalTitle = jsonLd?.title || titleHtml || null;
                        const finalCompany = jsonLd?.company || companyHtml || null;
                        const finalLocation = jsonLd?.location || locationHtml || null;

                        const record = {
                            url: detailUrl,
                            title: finalTitle,
                            company: finalCompany,
                            location: finalLocation,
                            datePosted,
                            employmentType: jsonLd?.employmentType || null,
                            validThrough: jsonLd?.validThrough || null,
                            salary: jsonLd?.salary || null,
                            descriptionHtml: jsonLd?.descriptionHtml || descriptionHtml || null,
                            descriptionText: descriptionText || null,
                            language: pageLang,
                            source: 'rekrute.com',
                            scrapedAt: new Date().toISOString(),
                        };

                        await Dataset.pushData(record);
                        saved++;

                        log.info(
                            `Saved DETAIL ${saved}/${RESULTS_WANTED}: ${detailUrl}`
                        );
                    } catch (err) {
                        log.error(
                            `Failed to process DETAIL ${request.url}: ${err.message}`
                        );
                        if (session) session.retire();
                    }
                } else {
                    log.warning(
                        `Unknown label "${label}" for URL ${request.url}, skipping.`
                    );
                }
            },
        });

        await crawler.run();

        log.info('Crawl finished', {
            saved,
            RESULTS_WANTED,
            listPages,
            detailPages,
            queuedDetail,
        });
    } finally {
        await Actor.exit();
    }
}

main().catch((err) => {
    // eslint-disable-next-line no-console
    console.error(err);
    process.exit(1);
});
