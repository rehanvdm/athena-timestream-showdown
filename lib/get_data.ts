import {v4 as uuidv4} from "uuid";
import { faker } from '@faker-js/faker';
import { z } from 'zod';

export const SchemaPage = z.object({
  site: z.string(),
  user_id: z.string(),
  session_id: z.string(),
  page_id: z.string(),
  page_url: z.string(),
  page_opened_at: z.string(),
  page_opened_at_date: z.string().optional(),
  time_on_page: z.number(),
  country_iso: z.string().optional(),
  country_name: z.string().optional(),
  city_name: z.string().optional(),
  device_type: z.string().optional(),
  is_bot: z.boolean(),
  utm_source: z.string().optional(),
  utm_medium: z.string().optional(),
  utm_campaign: z.string().optional(),
  utm_term: z.string().optional(),
  utm_content: z.string().optional(),
  querystring: z.string().optional(),
  referrer: z.string().optional(),
});
export type Page = z.infer<typeof SchemaPage>;

export async function* getData(maxRows: number, secondsInPast: number = 0)
{
  const site = "showdown";

  let previousUser = uuidv4();
  let previousSession = uuidv4();
  let previousPageUrl = uuidv4();

  const probabilityOfSameUser = 0.5;
  const probabilityOfSameSession = 0.5;
  const probabilityOfSamePageUrl = 0.5;
  const startDate = new Date(Date.now()-(secondsInPast*1000));

  let events: any[] = [];
  for (let i = 0; i < maxRows; i++) {
    let userUuid = uuidv4();
    let sessionUuid = uuidv4();
    let pageUrl = uuidv4();

    userUuid = (Math.random() < probabilityOfSameUser) ? previousUser : uuidv4();
    if (userUuid === previousUser) {
      sessionUuid = (Math.random() < probabilityOfSameSession) ? previousSession : uuidv4();
      if (sessionUuid === previousSession) {
        pageUrl = (Math.random() < probabilityOfSamePageUrl) ? previousPageUrl : uuidv4();
        previousPageUrl = pageUrl;
      }
      previousSession = sessionUuid;
    }
    previousUser = userUuid;

    let referrer: string | undefined = undefined;
    if (Math.random() < 0.2) {
      if (Math.random() < 0.4)
        referrer = "google.com";
      else
        referrer = faker.internet.domainName();
    }

    let utm_source: string | undefined = undefined;
    if (Math.random() < 0.1)
      utm_source = faker.company.name();

    let utm_campaign: string | undefined = undefined;
    if (Math.random() < 0.1)
      utm_campaign = faker.commerce.product();

    const pageUuid = uuidv4();
    let event: Page = {
      site: site,
      user_id: userUuid,
      session_id: sessionUuid,
      page_id: pageUuid,
      page_url: "/" + pageUrl + ".html",
      page_opened_at_date: startDate.toISOString().slice(0, 10),
      page_opened_at: (new Date(startDate.getTime() + i)).toISOString(), //Just adding a milli second for each call
      time_on_page: Math.floor(1 + (Math.random() * 60)),
      referrer: referrer,
      utm_source: utm_source,
      utm_campaign: utm_campaign,
      is_bot: false,
      country_iso: faker.address.countryCode(),
      country_name: faker.address.country(),
      city_name: faker.address.city(),
      device_type: Math.random() > 0.5 ? "desktop" : "mobile",
    };
    events.push(event);

    if(events.length >= 100) {
      yield events;
      events = [];
    }
  }
}